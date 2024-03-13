"""
Author: wanghui01
Date: 2024/03/12
Note: Get cpu info from machine via ipmi.
"""

import os
import re
import sys
import time
import json
import asyncio
import logging
import subprocess
import pandas as pd

SEP = os.path.sep
BASE_DIR = SEP.join(os.path.abspath(__file__).split(SEP)[:-1])
CUR_TIME = time.strftime("%Y%m%d%H%M%S", time.localtime())
LOG_DIR = f"{BASE_DIR}{SEP}logs"

if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

## define logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
# console_handler.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(f"{LOG_DIR}{SEP}{CUR_TIME}.log")
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


## custom exception
class RemoteBMCConnectException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


## check command return code
def check_cmd_rc(cmd, step_msg):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    # result = os.system(cmd)

    if result.returncode != 0:
        logger.error(
            f"Error code: {result.returncode}. Message: {result.stderr}. STEP: {step_msg}. CMD: {cmd}."
        )
        return False

    return True


## execute ipmitool command
def ipmi_cmd(ip_address, username, passwd, sub_command):
    output = ""

    ipmitool_cmd = (
        f"ipmitool -I lanplus -H {ip_address} -U {username} -P {passwd} lan print 1"
    )
    if not check_cmd_rc(
        cmd=ipmitool_cmd, step_msg="Check whether the remote BMC connection is normal"
    ):
        raise RemoteBMCConnectException(f"BMC: {ip_address} can't connect.")

    ipmitool_cmd = f"ipmitool -I lanplus -H {ip_address} -U {username} -P {passwd} -C 17 {sub_command}"

    result = subprocess.run(ipmitool_cmd, capture_output=True, shell=True, text=True)
    if result.returncode == 0:
        output = result.stdout
    else:
        output = f"{ip_address} capture volts fail, Error Code: {result.returncode}"
        logger.error(result.stderr)

    return output


## environment check
def check_native_ipmitool():
    if not check_cmd_rc(
        "ipmitool -V", step_msg="Check if the ipmitool tool is installed"
    ):
        sys.exit(2)


## load bmc info from hosts.json
def load_hosts_from_json(host_file_path):
    if not os.path.exists(host_file_path):
        logger.error(f"{host_file_path} not exist.")
        sys.exit(3)
    else:
        with open(host_file_path) as host_file:
            hosts = json.load(host_file)
    return hosts


def load_hosts_from_xlsx(host_file_path):
    if not os.path.exists(host_file_path):
        logger.error(f"{host_file_path} not exist.")
        sys.exit(3)
    else:
        df = pd.read_excel(host_file_path)
    return df.values
    # return hosts


def get_sn_from_bmc(bmc_ip, user):
    try:
        ## Only Linux
        # sn = ipmi_cmd(
        #     bmc_ip,
        #     user["username"],
        #     user["passwd"],
        #     "fru print 0 | grep 'Product Serial' | awk '{print $4}'",
        # )
        # sn = [x for x in sn.split("\n") if x != ""]

        outputs = ipmi_cmd(
            bmc_ip,
            user["username"],
            user["passwd"],
            "fru print 0 ",
        )
        outputs = re.findall("Product Serial.*", outputs)
        outputs = [[y for y in x.split(" ") if y != "" and y != ":"] for x in outputs]
        sn = outputs[0][-1]

    except RemoteBMCConnectException as rbmce:
        logger.debug(rbmce)

    # if len(sn) == 1:
    #     sn = sn[0]

    if sn:
        pass
    else:
        sn = "unknown"
    return sn


## run ipmi command
def get_cpu_v_from_bmc(bmc_ip, user):
    try:
        ## Only Linux Code
        # cpu_v = ipmi_cmd(
        #     bmc_ip,
        #     user["username"],
        #     user["passwd"],
        #     "sdr elist | grep VDDAVS | awk '{print $10}'",
        # )
        # cpu_v = [x for x in cpu_v.split("\n") if x != ""]

        cpu_v = []
        outputs = ipmi_cmd(
            bmc_ip,
            user["username"],
            user["passwd"],
            "sdr elist",
        )

        outputs = re.findall("VDDAVS.*", outputs)
        outputs = [[y for y in x.split(" ") if y != "" and y != "|"] for x in outputs]
        # print(" ")

        for output in outputs:
            cpu_v.append(output[-2])

    except RemoteBMCConnectException as rbmce:
        logger.debug(rbmce)

    if len(cpu_v) > 0:
        pass
    else:
        cpu_v = "unknown"

    return cpu_v


async def send_command_task(data, bmc_ip, user):
    sn = get_sn_from_bmc(bmc_ip, user)
    data["sn"].append(sn)
    data["bmc_ip"].append(bmc_ip)
    cpu_v_multi = []
    for _ in range(3):
        cpus_v = get_cpu_v_from_bmc(bmc_ip, user)
        cpu_v = min(cpus_v)
        cpus_v = ", ".join(cpus_v)
        logger.debug(f"{bmc_ip}, {sn}, {cpus_v}")
        cpu_v_multi.append(cpu_v)
        # wait 5 min.
        await asyncio.sleep(10)
    data["min_voltage"].append(min(cpu_v_multi))
    data["index"] += 1


async def send_command_after_delay(hosts):
    # cpus_v_col = ["cpus_v", "cpus_v_5min", "cpus_v_10min"]
    data = {"index": 0, "bmc_ip": [], "sn": [], "min_voltage": []}
    # cpus_v_col = [x for x in data.keys() if "cpus" in x]
    tasks = []
    for host in hosts:
        bmc_ip = host[0]
        user = {"username": host[1], "passwd": host[2]}
        logger.debug(
            f"bmc: {bmc_ip}, username: {user['username']}, passwd: {user['passwd']}"
        )

        tasks.append(asyncio.create_task(send_command_task(data, bmc_ip, user)))

    for task in tasks:
        await task
    return data


if __name__ == "__main__":
    start_time = time.time()
    check_native_ipmitool()

    # check host.xlsx
    host_file_path = BASE_DIR + SEP + "hosts.xlsx"
    hosts = load_hosts_from_xlsx(host_file_path)

    ## run command
    # for host in hosts:
    #     bmc_ip = host[0]
    #     user = {"username": host[1], "passwd": host[2]}
    #     sn, cpu_v = get_cpu_v_from_bmc(bmc_ip, user)

    #     data["bmc_ip"].append(bmc_ip)
    #     data["sn"].append(sn[0])
    #     data["cpu_v"].append(", ".join(cpu_v))
    #     data["index"] += 1

    data = asyncio.run(send_command_after_delay(hosts))

    machine_number = data["index"]
    del data["index"]
    df = pd.DataFrame(data)
    df.index += 1
    if machine_number <= 30:
        pass
    else:
        # sort data and limit 30.
        df = df.sort_values(by="min_voltage")
        df = df.head(30)
    df.to_excel("output.xlsx")
    end_time = time.time()
    execute_time = int(end_time - start_time)
    logger.debug(f"Finished: {execute_time}s")
