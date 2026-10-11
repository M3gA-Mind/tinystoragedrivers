//! Synchronous single-secret operations for a trusted native execution owner.

use super::{
    KEY_LEN, crypto, io_error, next_temp_seq, open_parent, require_utf8, secure, validate_name,
};
use serde::ser::{SerializeMap, Serializer};
use std::time::Instant;
use std::{
    fmt,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tinystoragedrivers_core::{Result, StorageError};
use zeroize::Zeroizing;

/// Finite per-operation ceilings. Zero ceilings reject any corresponding use.
#[derive(Clone, Copy, Debug)]
pub struct FileTransactionBudget {
    /// Maximum stored ciphertext bytes, including nonce and tag.
    pub ciphertext_bytes: usize,
    /// Maximum decrypted or serialized JSON bytes.
    pub json_bytes: usize,
    /// Maximum distinct map entries.
    pub entries: usize,
    /// Maximum UTF-8 name bytes.
    pub name_bytes: usize,
    /// Maximum UTF-8 value bytes.
    pub value_bytes: usize,
    /// Conservative simultaneous buffer and map allocation reservation.
    pub allocation_bytes: usize,
    /// Maximum returned plaintext bytes.
    pub output_bytes: usize,
    /// Absolute deadline for lock acquisition and subsequent admission checks.
    pub deadline: Instant,
}

/// The filesystem phase at which the native owner must revalidate authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileTransactionPhase {
    /// Before trying to acquire the file lock, including each retry.
    Lock,
    /// After acquiring and verifying the file lock.
    Locked,
    /// After reading and validating the encrypted map.
    Read,
    /// Before staging encrypted bytes.
    Stage,
    /// Before rename, held through exact locked readback.
    Publish,
    /// Before returning absence, plaintext, or mutation acknowledgment.
    Output,
}

/// An owner-provided scoped authority gate, released on drop.
///
/// The implementation must serialize revocation/generation changes with any
/// held lease. A publication lease covers rename, flush and exact readback.
/// This driver does not interpret consent or retain authority after return.
pub trait FileTransactionLease {}

/// Trusted native authority/consent hook, implemented by the execution owner.
///
/// This is not a caller-supplied boolean or receipt. The owner binds this object
/// privately to the exact admitted resource, name, operation and live authority.
pub trait FileTransactionGuard {
    /// Acquire a live phase lease. Must be bounded and must check cancellation.
    ///
    /// # Errors
    /// Return a sanitized error when authority, consent or execution is denied.
    fn enter(&self, phase: FileTransactionPhase) -> Result<Box<dyn FileTransactionLease + '_>>;
}

/// One operation; request data remains borrowed for the actual synchronous job.
#[derive(Clone, Copy)]
pub enum FileTransactionOperation<'a> {
    /// Read one named secret.
    Get(&'a str),
    /// Store one UTF-8 secret.
    Set(&'a str, &'a [u8]),
    /// Remove one named secret.
    Delete(&'a str),
}

impl fmt::Debug for FileTransactionOperation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileTransactionOperation([redacted])")
    }
}

/// Result of one operation. Plaintext is wiped when dropped.
pub enum FileTransactionResult {
    /// Present plaintext or confirmed absence.
    Get(Option<Zeroizing<Vec<u8>>>),
    /// Set acknowledged after exact locked readback.
    Set,
    /// Whether an entry was removed; a mutation requires exact locked readback.
    Delete(bool),
}

impl fmt::Debug for FileTransactionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileTransactionResult([redacted])")
    }
}

/// Immutable exact file resource, retaining its admitted directory and lock.
///
/// External data/lock replacement invalidates this resource. Reinitialize only
/// under the owner's initialization authority; never treat rejection as absence.
/// No key, map or asynchronous worker is retained here.
pub struct FileTransactionResource {
    path: PathBuf,
    lock: secure::Locked,
    data: Option<File>,
    fenced: bool,
}

impl fmt::Debug for FileTransactionResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FileTransactionResource([redacted])")
    }
}

impl FileTransactionResource {
    /// Admit an existing private directory and exact data/sidecar resource.
    /// Creates the private sidecar when absent; never creates a directory/key.
    /// Does not acquire the advisory lock or read plaintext.
    ///
    /// # Errors
    /// Reject unsafe names, directories, symlinks, permissions and I/O failures.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let namespace = open_parent(&path, false)?;
        let mut name = secure::file_name(&path)
            .map_err(|e| io_error("admit the transaction name", e))?
            .as_os_str()
            .to_os_string();
        name.push(".lock");
        let name = PathBuf::from(name);
        let file = match namespace.create_file(&name) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => namespace
                .open_file(&name, true)
                .map_err(|e| io_error("open the transaction lock", e))?,
            Err(error) => return Err(io_error("create the transaction lock", error)),
        };
        let data = open_data(&namespace, &path)?;
        Ok(Self {
            path,
            lock: secure::Locked {
                namespace,
                file,
                name,
            },
            data,
            fenced: false,
        })
    }

    /// Perform one operation inline, borrowing the zeroizing key only until return.
    /// The caller owns worker admission, key accounting and physical quiescence.
    /// No hidden worker or queue is spawned. OS I/O itself cannot be canceled;
    /// denial between phases prevents ordinary publication/acknowledgment.
    ///
    /// # Errors
    /// Report invalid input, exhausted budgets/deadline, owner denial, changed
    /// resources, corruption, or I/O failure. An error after rename is an
    /// uncertain visible mutation; the owner must fence retries until resolved.
    pub fn execute(
        &mut self,
        key: &Zeroizing<[u8; KEY_LEN]>,
        operation: FileTransactionOperation<'_>,
        budget: FileTransactionBudget,
        guard: &dyn FileTransactionGuard,
    ) -> Result<FileTransactionResult> {
        if self.fenced {
            return Err(StorageError::conflict("transaction resource is fenced"));
        }
        budget.admit()?;
        let name = match operation {
            FileTransactionOperation::Get(n)
            | FileTransactionOperation::Set(n, _)
            | FileTransactionOperation::Delete(n) => n,
        };
        validate_name(name)?;
        limit(name.len(), budget.name_bytes)?;
        if let FileTransactionOperation::Set(_, value) = operation {
            limit(value.len(), budget.value_bytes)?;
            require_utf8(value, "encrypted_file")?;
        }
        let _held = acquire(&self.lock.file, &budget, guard)?;
        let lease = phase(&budget, guard, FileTransactionPhase::Locked)?;
        self.verify()?;
        let blob = self.read(budget.ciphertext_bytes)?;
        let mut map = if blob.is_empty() {
            BoundedMap::default()
        } else {
            limit(
                blob.len()
                    .saturating_sub(crypto::NONCE_LEN + crypto::TAG_LEN),
                budget.json_bytes,
            )?;
            let json = crypto::decrypt(key, &blob)?;
            decode(&json, &budget)?
        };
        self.verify()?;
        drop(lease);
        drop(phase(&budget, guard, FileTransactionPhase::Read)?);
        let result = match operation {
            FileTransactionOperation::Get(_) => {
                let value = map
                    .0
                    .iter()
                    .find(|(n, _)| n.as_str() == name)
                    .map(|(_, v)| v.as_bytes());
                if let Some(value) = value {
                    limit(value.len(), budget.output_bytes)?;
                }
                FileTransactionResult::Get(value.map(|v| Zeroizing::new(v.to_vec())))
            }
            FileTransactionOperation::Set(_, value) => {
                let value = require_utf8(value, "encrypted_file")?;
                if let Some((_, old)) = map.0.iter_mut().find(|(n, _)| n.as_str() == name) {
                    *old = Zeroizing::new(value.to_owned());
                } else {
                    limit(map.0.len().saturating_add(1), budget.entries)?;
                    map.0.push((
                        Zeroizing::new(name.to_owned()),
                        Zeroizing::new(value.to_owned()),
                    ));
                }
                self.publish(key, &map, &budget, guard)?;
                FileTransactionResult::Set
            }
            FileTransactionOperation::Delete(_) => {
                let removed = map
                    .0
                    .iter()
                    .position(|(n, _)| n.as_str() == name)
                    .map(|i| map.0.remove(i))
                    .is_some();
                if removed {
                    self.publish(key, &map, &budget, guard)?;
                }
                FileTransactionResult::Delete(removed)
            }
        };
        let _output = phase(&budget, guard, FileTransactionPhase::Output)?;
        self.verify()?;
        self.fenced = false;
        Ok(result)
    }

    fn verify(&self) -> Result<()> {
        self.lock
            .verify()
            .map_err(|e| io_error("verify the transaction lock", e))?;
        self.lock
            .namespace
            .verify()
            .map_err(|e| io_error("verify the transaction namespace", e))?;
        let observed = open_data(&self.lock.namespace, &self.path)?;
        match (&self.data, observed) {
            (None, None) => Ok(()),
            (Some(expected), Some(observed))
                if secure::identity(expected)
                    .map_err(|e| io_error("inspect transaction identity", e))?
                    == secure::identity(&observed)
                        .map_err(|e| io_error("inspect transaction identity", e))? =>
            {
                Ok(())
            }
            _ => Err(StorageError::conflict("transaction data object changed")),
        }
    }

    fn read(&self, maximum: usize) -> Result<Zeroizing<Vec<u8>>> {
        #[cfg(test)]
        tests::before_read_open();
        let observed = open_data(&self.lock.namespace, &self.path)?;
        #[cfg(test)]
        tests::after_read_open();
        // Authenticate the handle that supplies ciphertext, not a separate
        // lookup before/after the read. Publication installs the staging identity
        // in self.data before calling this same exact-resource reader.
        match (&self.data, &observed) {
            (None, None) => {}
            (Some(expected), Some(file))
                if secure::identity(expected)
                    .map_err(|e| io_error("inspect retained transaction identity", e))?
                    == secure::identity(file)
                        .map_err(|e| io_error("inspect read transaction identity", e))? =>
            {
                #[cfg(unix)]
                super::unix::verify_file(file)
                    .map_err(|e| io_error("verify transaction read handle", e))?;
                #[cfg(windows)]
                super::windows::verify_file(file)
                    .map_err(|e| io_error("verify transaction read handle", e))?;
            }
            _ => return Err(StorageError::conflict("transaction read object changed")),
        }
        let result = match observed {
            None => Ok(Zeroizing::new(Vec::new())),
            Some(file) => read_bounded(file, maximum),
        };
        #[cfg(test)]
        tests::after_read();
        result
    }

    fn publish(
        &mut self,
        key: &Zeroizing<[u8; KEY_LEN]>,
        map: &BoundedMap,
        budget: &FileTransactionBudget,
        guard: &dyn FileTransactionGuard,
    ) -> Result<()> {
        let mut json = BoundedWriter {
            bytes: Zeroizing::new(Vec::with_capacity(budget.json_bytes)),
            maximum: budget.json_bytes,
        };
        // Serialize references; never duplicate the decrypted map.
        let mut serializer = serde_json::Serializer::new(&mut json);
        let mut object = serializer
            .serialize_map(Some(map.0.len()))
            .map_err(|_| malformed())?;
        for (name, value) in &map.0 {
            object
                .serialize_entry(name.as_str(), value.as_str())
                .map_err(|_| exhausted())?;
        }
        object.end().map_err(|_| exhausted())?;
        limit(
            json.bytes
                .len()
                .saturating_add(crypto::NONCE_LEN + crypto::TAG_LEN),
            budget.ciphertext_bytes,
        )?;
        let blob = crypto::encrypt(key, &json.bytes)?;
        let lease = phase(budget, guard, FileTransactionPhase::Stage)?;
        self.verify()?;
        let staged = Staged::new(&self.lock.namespace, &self.path, &blob)?;
        drop(lease);
        let lease = phase(budget, guard, FileTransactionPhase::Publish)?;
        self.verify()?;
        staged.verify()?;
        check_deadline(budget)?;
        self.fenced = true;
        self.lock
            .namespace
            .replace(
                &staged.name,
                secure::file_name(&self.path)
                    .map_err(|e| io_error("admit the transaction target", e))?,
            )
            .map_err(|e| io_error("publish the transaction", e))?;
        #[cfg(test)]
        tests::after_publish();
        self.data = open_data(&self.lock.namespace, &self.path)?;
        let published = self
            .data
            .as_ref()
            .ok_or_else(|| StorageError::conflict("published transaction data is absent"))?;
        if secure::identity(published).map_err(|e| io_error("inspect published transaction", e))?
            != secure::identity(&staged.file)
                .map_err(|e| io_error("inspect staged transaction", e))?
        {
            return Err(StorageError::conflict(
                "published transaction identity changed",
            ));
        }
        published
            .sync_all()
            .map_err(|e| io_error("flush the published transaction", e))?;
        #[cfg(unix)]
        self.lock
            .namespace
            .sync()
            .map_err(|e| io_error("persist the transaction namespace", e))?;
        if self.read(budget.ciphertext_bytes)?.as_slice() != blob {
            return Err(StorageError::conflict(
                "published transaction readback differs",
            ));
        }
        self.verify()?;
        drop(lease);
        Ok(())
    }
}

fn open_data(namespace: &secure::Namespace, path: &Path) -> Result<Option<File>> {
    match namespace.open_file(
        secure::file_name(path).map_err(|e| io_error("admit the transaction name", e))?,
        false,
    ) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("open the transaction data", error)),
    }
}

fn exhausted() -> StorageError {
    StorageError::invalid_input("secret transaction budget exhausted")
}
fn malformed() -> StorageError {
    StorageError::serialization("decrypted secrets file is not a json object of strings")
}
fn limit(actual: usize, maximum: usize) -> Result<()> {
    if actual > maximum {
        Err(exhausted())
    } else {
        Ok(())
    }
}

impl FileTransactionBudget {
    fn admit(&self) -> Result<()> {
        // Charge simultaneous crypto buffers, map/decoder scratch, serialization,
        // request copies and output, including conservative map entry overhead.
        let required = self
            .ciphertext_bytes
            .checked_mul(4)
            .and_then(|n| {
                self.json_bytes
                    .checked_mul(8)
                    .and_then(|j| n.checked_add(j))
            })
            .and_then(|n| self.entries.checked_mul(512).and_then(|e| n.checked_add(e)))
            .and_then(|n| n.checked_add(self.name_bytes))
            .and_then(|n| n.checked_add(self.value_bytes))
            .and_then(|n| n.checked_add(self.output_bytes))
            .and_then(|n| n.checked_add(KEY_LEN))
            .ok_or_else(exhausted)?;
        limit(required, self.allocation_bytes)
    }
}

fn phase<'a>(
    budget: &FileTransactionBudget,
    guard: &'a dyn FileTransactionGuard,
    phase: FileTransactionPhase,
) -> Result<Box<dyn FileTransactionLease + 'a>> {
    check_deadline(budget)?;
    let lease = guard.enter(phase)?;
    check_deadline(budget)?;
    Ok(lease)
}

fn check_deadline(budget: &FileTransactionBudget) -> Result<()> {
    if Instant::now() >= budget.deadline {
        return Err(StorageError::unavailable(
            "secret transaction deadline expired",
        ));
    }
    Ok(())
}

struct Held(File);
impl Drop for Held {
    fn drop(&mut self) {
        let _ = fs4::FileExt::unlock(&self.0);
    }
}
fn acquire(
    file: &File,
    budget: &FileTransactionBudget,
    guard: &dyn FileTransactionGuard,
) -> Result<Held> {
    let file = file
        .try_clone()
        .map_err(|e| io_error("retain the transaction lock", e))?;
    loop {
        let lease = phase(budget, guard, FileTransactionPhase::Lock)?;
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => return Ok(Held(file)),
            Err(fs4::TryLockError::WouldBlock) => {}
            Err(fs4::TryLockError::Error(error)) => {
                return Err(io_error("try the transaction lock", error));
            }
        }
        drop(lease);
        std::thread::sleep(
            std::time::Duration::from_millis(1)
                .min(budget.deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn read_bounded(mut file: File, maximum: usize) -> Result<Zeroizing<Vec<u8>>> {
    limit(
        usize::try_from(
            file.metadata()
                .map_err(|e| io_error("inspect transaction length", e))?
                .len(),
        )
        .map_err(|_| exhausted())?,
        maximum,
    )?;
    let mut bytes = Zeroizing::new(vec![0; maximum]);
    let mut length = 0;
    while length < maximum {
        let count = file
            .read(&mut bytes[length..])
            .map_err(|e| io_error("read bounded transaction data", e))?;
        if count == 0 {
            break;
        }
        length += count;
    }
    let mut extra = Zeroizing::new([0; 1]);
    if file
        .read(&mut extra[..])
        .map_err(|e| io_error("verify bounded transaction length", e))?
        != 0
    {
        return Err(exhausted());
    }
    bytes.truncate(length);
    Ok(bytes)
}

struct BoundedWriter {
    bytes: Zeroizing<Vec<u8>>,
    maximum: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("serialization budget exhausted"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Staged<'a> {
    namespace: &'a secure::Namespace,
    name: PathBuf,
    file: File,
}
impl<'a> Staged<'a> {
    fn new(namespace: &'a secure::Namespace, path: &Path, bytes: &[u8]) -> Result<Self> {
        Self::with_sequence(namespace, path, bytes, &mut next_temp_seq)
    }

    fn with_sequence(
        namespace: &'a secure::Namespace,
        path: &Path,
        bytes: &[u8],
        sequence: &mut dyn FnMut() -> u64,
    ) -> Result<Self> {
        for _ in 0..16 {
            let mut name = secure::file_name(path)
                .map_err(|e| io_error("admit the staging name", e))?
                .as_os_str()
                .to_os_string();
            name.push(format!(".{}.{}.tmp", std::process::id(), sequence()));
            let name = PathBuf::from(name);
            let file = match namespace.create_file(&name) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(io_error("reserve transaction staging", error)),
            };
            let mut staged = Self {
                namespace,
                name,
                file,
            };
            staged
                .file
                .write_all(bytes)
                .map_err(|e| io_error("write transaction staging", e))?;
            staged
                .file
                .sync_all()
                .map_err(|e| io_error("flush transaction staging", e))?;
            staged.verify()?;
            return Ok(staged);
        }
        Err(StorageError::unavailable(
            "transaction staging collisions exhausted",
        ))
    }
    fn verify(&self) -> Result<()> {
        let opened = self
            .namespace
            .open_file(&self.name, false)
            .map_err(|e| io_error("verify transaction staging", e))?;
        if secure::identity(&opened).map_err(|e| io_error("inspect transaction staging", e))?
            != secure::identity(&self.file)
                .map_err(|e| io_error("inspect transaction staging", e))?
        {
            return Err(StorageError::conflict("transaction staging object changed"));
        }
        Ok(())
    }
}
impl Drop for Staged<'_> {
    fn drop(&mut self) {
        if self.verify().is_ok() {
            let _ = self.namespace.remove(&self.name);
        }
    }
}

#[derive(Default)]
struct BoundedMap(Vec<(Zeroizing<String>, Zeroizing<String>)>);

// A string-only JSON parser keeps every decoded string and scratch buffer in
// zeroizing storage, including malformed-input and budget failures.
struct Decoder<'a> {
    text: &'a [u8],
    position: usize,
}
impl Decoder<'_> {
    fn whitespace(&mut self) {
        while self
            .text
            .get(self.position)
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.position += 1;
        }
    }
    fn take(&mut self, byte: u8) -> Result<()> {
        self.whitespace();
        if self.text.get(self.position) != Some(&byte) {
            return Err(malformed());
        }
        self.position += 1;
        Ok(())
    }
    fn hex4(&mut self) -> Result<u32> {
        let mut value = 0;
        for _ in 0..4 {
            let b = *self.text.get(self.position).ok_or_else(malformed)?;
            self.position += 1;
            value = value * 16
                + match b {
                    b'0'..=b'9' => u32::from(b - b'0'),
                    b'a'..=b'f' => u32::from(b - b'a' + 10),
                    b'A'..=b'F' => u32::from(b - b'A' + 10),
                    _ => return Err(malformed()),
                };
        }
        Ok(value)
    }
    fn string(&mut self, maximum: usize) -> Result<Zeroizing<String>> {
        self.take(b'"')?;
        let mut out = Zeroizing::new(Vec::with_capacity(maximum.min(self.text.len())));
        loop {
            let byte = *self.text.get(self.position).ok_or_else(malformed)?;
            self.position += 1;
            match byte {
                b'"' => break,
                0..=31 => return Err(malformed()),
                b'\\' => {
                    let escape = *self.text.get(self.position).ok_or_else(malformed)?;
                    self.position += 1;
                    match escape {
                        b'"' | b'\\' | b'/' => {
                            limit(out.len().saturating_add(1), maximum)?;
                            out.push(escape);
                        }
                        b'b' | b'f' | b'n' | b'r' | b't' => {
                            limit(out.len().saturating_add(1), maximum)?;
                            out.push(match escape {
                                b'b' => 8,
                                b'f' => 12,
                                b'n' => 10,
                                b'r' => 13,
                                _ => 9,
                            });
                        }
                        b'u' => {
                            let mut value = self.hex4()?;
                            if (0xD800..=0xDBFF).contains(&value) {
                                if self.text.get(self.position..self.position + 2) != Some(b"\\u") {
                                    return Err(malformed());
                                }
                                self.position += 2;
                                let low = self.hex4()?;
                                if !(0xDC00..=0xDFFF).contains(&low) {
                                    return Err(malformed());
                                }
                                value = 0x10000 + ((value - 0xD800) << 10) + low - 0xDC00;
                            }
                            let ch = char::from_u32(value).ok_or_else(malformed)?;
                            let mut encoded = Zeroizing::new([0; 4]);
                            let bytes = ch.encode_utf8(&mut *encoded).as_bytes();
                            limit(out.len().saturating_add(bytes.len()), maximum)?;
                            out.extend_from_slice(bytes);
                        }
                        _ => return Err(malformed()),
                    }
                }
                _ => {
                    limit(out.len().saturating_add(1), maximum)?;
                    out.push(byte);
                }
            }
        }
        let text = std::str::from_utf8(&out).map_err(|_| malformed())?;
        Ok(Zeroizing::new(text.to_owned()))
    }
}
fn decode(json: &[u8], budget: &FileTransactionBudget) -> Result<BoundedMap> {
    limit(json.len(), budget.json_bytes)?;
    let mut decoder = Decoder {
        text: json,
        position: 0,
    };
    let mut map = BoundedMap(Vec::with_capacity(budget.entries));
    decoder.take(b'{')?;
    decoder.whitespace();
    if decoder.text.get(decoder.position) != Some(&b'}') {
        loop {
            let name = decoder.string(budget.name_bytes)?;
            validate_name(&name)?;
            decoder.take(b':')?;
            let value = decoder.string(budget.value_bytes)?;
            if let Some((_, old)) = map.0.iter_mut().find(|(n, _)| **n == *name) {
                *old = value;
            } else {
                limit(map.0.len().saturating_add(1), budget.entries)?;
                map.0.push((name, value));
            }
            decoder.whitespace();
            if decoder.text.get(decoder.position) != Some(&b',') {
                break;
            }
            decoder.position += 1;
        }
    }
    decoder.take(b'}')?;
    decoder.whitespace();
    if decoder.position != json.len() {
        return Err(malformed());
    }
    Ok(map)
}

#[cfg(test)]
#[path = "transaction_tests.rs"]
mod tests;
