#!/usr/bin/env python3
"""Launcher isolation checks; these never contact a provider or start a daemon."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, call, patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("native_secret_service", Path(__file__).with_name("check-native-secret-service.py"))
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


class IsolationTests(unittest.TestCase):
    def test_child_environment_drops_operator_bus_and_gui_and_preserves_home(self):
        parent = {"HOME": "/public-home-fixture", "DBUS_SESSION_BUS_ADDRESS": "public-bus-fixture",
                  "DBUS_STARTER_ADDRESS": "public-starter", "DBUS_STARTER_BUS_TYPE": "session",
                  "GNOME_KEYRING_CONTROL": "public-control", "DISPLAY": ":99", "WAYLAND_DISPLAY": "public-display"}
        with tempfile.TemporaryDirectory(dir=launcher.ROOT / "target") as directory:
            root = Path(directory)
            with patch.dict(os.environ, parent, clear=True):
                child = launcher.private_environment(root)
                self.assertEqual(dict(os.environ), parent)
            self.assertEqual(child["HOME"], parent["HOME"])
            for name in parent.keys() - {"HOME"}:
                self.assertNotIn(name, child)
            for name in ("XDG_DATA_HOME", "XDG_RUNTIME_DIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME"):
                self.assertEqual(Path(child[name]).parent, root)
                self.assertEqual(Path(child[name]).stat().st_mode & 0o777, 0o700)

    def test_timeout_kills_descendant_group_even_when_session_leader_already_exited(self):
        process = Mock(pid=1234)
        process.wait.return_value = 0
        with patch.object(launcher.os, "killpg") as kill:
            launcher.terminate_owned_group(process)
        self.assertEqual(kill.call_args_list, [call(1234, launcher.signal.SIGTERM), call(1234, launcher.signal.SIGKILL)])
        self.assertEqual(process.wait.call_count, 2)

    def test_receipts_require_exact_fixed_boolean_fields(self):
        receipt = dict.fromkeys(launcher.RECEIPT_FIELDS, True)
        self.assertTrue(launcher.valid_receipt(receipt))
        self.assertFalse(launcher.valid_receipt({**receipt, "unexpected": True}))
        receipt["private_bus_owner"] = 1
        self.assertFalse(launcher.valid_receipt(receipt))

    def test_bus_configuration_cannot_activate_an_operator_service(self):
        from xml.etree import ElementTree
        config = ElementTree.fromstring(launcher.bus_configuration())
        self.assertEqual(config.findtext("type"), "session")
        self.assertEqual(config.findtext("auth"), "EXTERNAL")
        self.assertEqual(config.findall("servicedir"), [])
        self.assertEqual(config.findall("standard_session_servicedirs"), [])


if __name__ == "__main__":
    unittest.main()
