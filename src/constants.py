# Copyright 2024 Omnivector, LLC.
# See LICENSE file for licensing details.
"""This module provides constants for the slurmctld-operator charm."""
from pathlib import Path

SLURM_SNAP_PATH = Path("/var/snap/slurm")
SLURM_SNAP_COMMON_PATH = SLURM_SNAP_PATH / "common"
SLURM_USER = "slurm"
SLURM_GROUP = "slurm"

CHARM_MAINTAINED_SLURM_CONF_PARAMETERS = {
    "AuthAltParameters": "jwt_key=/var/spool/slurmctld/jwt_hs256.key",
    "AuthAltTypes": "auth/jwt",
    "AuthInfo": SLURM_SNAP_COMMON_PATH / "run/munge/munged.socket.2",
    "AuthType": "auth/munge",
    "GresTypes": "gpu",
    "HealthCheckInterval": "600",
    "HealthCheckNodeState": "ANY,CYCLE",
    "HealthCheckProgram": "/usr/sbin/omni-nhc-wrapper",
    "MailProg": "/usr/bin/mail.mailutils",
    "PluginDir": "/usr/lib/x86_64-linux-gnu/slurm-wlm",
    "PlugStackConfig": "/etc/slurm/plugstack.conf.d/plugstack.conf",
    "SelectType": "select/cons_tres",
    "SlurmctldPort": "6817",
    "SlurmdPort": "6818",
    "StateSaveLocation": "/var/spool/slurmctld",
    "SlurmdSpoolDir": "/var/spool/slurmd",
    "SlurmctldParameters": "enable_configless",
    "SlurmctldLogFile": SLURM_SNAP_COMMON_PATH / "var/log/slurm/slurmctld.log",
    "SlurmdLogFile": SLURM_SNAP_COMMON_PATH / "var/log/slurm/slurmctld.log",
    "SlurmdPidFile": "/run/slurmd.pid",
    "SlurmctldPidFile": "/run/slurmctld.pid",
    "SlurmUser": SLURM_USER,
    "SlurmdUser": "root",
    "RebootProgram": '"/usr/sbin/reboot --reboot"',
}
