import btrfs
import sys
import time
import json
import re
from subprocess import Popen, PIPE
from prometheus_client import start_http_server, Gauge
# g = Gauge('btrfs_device_used_bytes', 'btrfs_device_used_bytes',['filesystem_path', 'device'])
total_bytes_g = Gauge('btrfs_device_total_bytes', 'btrfs_device_total_bytes', [
                      'filesystem_path', 'device_id', 'device_path'])
used_bytes_g = Gauge('btrfs_device_used_bytes', 'btrfs_device_used_bytes', [
                     'filesystem_path', 'device_id', 'device_path'])
write_errs = Gauge('btrfs_device_write_err', 'btrfs_device_write_err', [
                   'filesystem_path', 'device_id', 'device_path'])
read_errs = Gauge('btrfs_device_read_err', 'btrfs_device_read_err', [
                  'filesystem_path', 'device_id', 'device_path'])
flush_errs = Gauge('btrfs_device_flush_err', 'btrfs_device_flush_err', [
                   'filesystem_path', 'device_id', 'device_path'])
generation_errs = Gauge('btrfs_device_generation_err', 'btrfs_device_generation_err', [
                        'filesystem_path', 'device_id', 'device_path'])
corruption_errs = Gauge('btrfs_device_corruption_err', 'btrfs_device_corruption_err', [
                        'filesystem_path', 'device_id', 'device_path'])

balance_status = Gauge('btrfs_balance_status',
                       'btrfs_balance_status', ['filesystem_path'])
balance_expected = Gauge('btrfs_balance_expected',
                         'btrfs_balance_expected', ['filesystem_path'])
balance_considered = Gauge('btrfs_balance_considered',
                           'btrfs_balance_considered', ['filesystem_path'])
balance_completed = Gauge('btrfs_balance_completed',
                          'btrfs_balance_completed', ['filesystem_path'])

scrub_status = Gauge('btrfs_scrub_status',
                     'btrfs_scrub_status', ['filesystem_path'])
scrub_data_extents_scrubbed = Gauge(
    'scrub_data_extents_scrubbed', 'scrub_data_extents_scrubbed', ['filesystem_path'])
scrub_tree_extents_scrubbed = Gauge(
    'scrub_tree_extents_scrubbed', 'scrub_tree_extents_scrubbed', ['filesystem_path'])
scrub_data_bytes_scrubbed = Gauge(
    'scrub_data_bytes_scrubbed', 'scrub_data_bytes_scrubbed', ['filesystem_path'])
scrub_tree_bytes_scrubbed = Gauge(
    'scrub_tree_bytes_scrubbed', 'scrub_tree_bytes_scrubbed', ['filesystem_path'])
scrub_read_errors = Gauge(
    'scrub_read_errors', 'scrub_read_errors', ['filesystem_path'])
scrub_csum_errors = Gauge(
    'scrub_csum_errors', 'scrub_csum_errors', ['filesystem_path'])
scrub_verify_errors = Gauge('scrub_verify_errors',
                            'scrub_verify_errors', ['filesystem_path'])
scrub_no_csum = Gauge('scrub_no_csum', 'scrub_no_csum', ['filesystem_path'])
scrub_csum_discards = Gauge('scrub_csum_discards',
                            'scrub_csum_discards', ['filesystem_path'])
scrub_super_errors = Gauge(
    'scrub_super_errors', 'scrub_super_errors', ['filesystem_path'])
scrub_malloc_errors = Gauge('scrub_malloc_errors',
                            'scrub_malloc_errors', ['filesystem_path'])
scrub_uncorrectable_errors = Gauge(
    'scrub_uncorrectable_errors', 'scrub_uncorrectable_errors', ['filesystem_path'])
scrub_unverified_errors = Gauge(
    'scrub_unverified_errors', 'scrub_unverified_errors', ['filesystem_path'])
scrub_corrected_errors = Gauge(
    'scrub_corrected_errors', 'scrub_corrected_errors', ['filesystem_path'])
scrub_last_physical = Gauge('scrub_last_physical',
                            'scrub_last_physical', ['filesystem_path'])


def exec_cmd(cmd):

    #     Log.debug("cmd: %s" %(cmd))
    content = ""
    try:
        p = Popen(cmd, bufsize=4096, stdout=PIPE, shell=True)

        while True:
            cont = p.stdout.read()
            if cont == b"":
                break
            content += cont.decode('utf-8')
        #     Log.debug("contLen: %d" %(len(cont)))
            time.sleep(1)
        retState = p.wait()

        return retState, content
    except Exception as e:
        # Log.err("(%s)" %(traceback.format_exc()))
        return 255, "cmd({}) err: {}".format(str(cmd), str(e))


def set_progress():
    for fs_path in btrfs.utils.mounted_filesystem_paths():
        set_balance(fs_path)
        set_deviceinfo(fs_path)
        set_scrub(fs_path)


def set_deviceinfo(fs_path):
    with btrfs.FileSystem(fs_path) as fs:
        fs_info = fs.fs_info()
        # print(fs_info)
        usage = fs.usage()
        # for t in fs.space_info():
        #     print(t)
        # btrfs.utils.pretty_print(usage)
        for device in fs.devices():
            # g.labels(filesystem_path=fs_path,device=device.devid).set(device.bytes_used)
            # print(device.fsid)
            info = fs.dev_info(device.devid)
            used_bytes_g.labels(filesystem_path=fs_path, device_id=device.devid,
                                device_path=info.path).set(info.bytes_used)
            total_bytes_g.labels(filesystem_path=fs_path, device_id=device.devid,
                                 device_path=info.path).set(info.total_bytes)
            stats = fs.dev_stats(device.devid)
            write_errs.labels(filesystem_path=fs_path, device_id=device.devid,
                              device_path=info.path).set(stats.write_errs)
            read_errs.labels(filesystem_path=fs_path, device_id=device.devid,
                             device_path=info.path).set(stats.read_errs)
            flush_errs.labels(filesystem_path=fs_path, device_id=device.devid,
                              device_path=info.path).set(stats.flush_errs)
            generation_errs.labels(filesystem_path=fs_path, device_id=device.devid,
                                   device_path=info.path).set(stats.generation_errs)
            corruption_errs.labels(filesystem_path=fs_path, device_id=device.devid,
                                   device_path=info.path).set(stats.corruption_errs)
        # for chunk in fs.chunks():
        #     print(chunk)


def set_balance(fs_path):
    with btrfs.FileSystem(fs_path) as fs:
        try:
            res = btrfs.ioctl.balance_progress(fs.fd)
            balance_status.labels(filesystem_path=fs_path).set(res.state)
            balance_completed.labels(
                filesystem_path=fs_path).set(res.completed)
            balance_expected.labels(filesystem_path=fs_path).set(res.expected)
            balance_considered.labels(
                filesystem_path=fs_path).set(res.considered)
        except btrfs.ioctl.BalanceError:
            balance_status.labels(filesystem_path=fs_path).set(0)


a = """
scrub status for d8bcd3aa-aec2-4e36-a756-95ced75d8481
        scrub started at Mon Jul 29 13:44:57 2019, running for 00:01:39
        data_extents_scrubbed: 6936133
        tree_extents_scrubbed: 45074
        data_bytes_scrubbed: 440326840320
        tree_bytes_scrubbed: 738492416
        read_errors: 0
        csum_errors: 0
        verify_errors: 0
        no_csum: 10752
        csum_discards: 0
        super_errors: 0
        malloc_errors: 0
        uncorrectable_errors: 0
        unverified_errors: 0
        corrected_errors: 0
        last_physical: 440764203008
"""


def parse_result_and_set_gauge(g, fs_path, result, key):
    b = re.search('{}: (?P<bytes>[0-9]+)\n'.format(key), result).group('bytes')
    # print(int(b))
    g.labels(filesystem_path=fs_path).set(int(b))


def set_scrub(fs_path):
    code, res = exec_cmd('btrfs scrub status -R {}'.format(fs_path))
    # print(res)
    parse_result_and_set_gauge(
        scrub_data_extents_scrubbed, fs_path, res, 'data_extents_scrubbed')
    parse_result_and_set_gauge(
        scrub_tree_extents_scrubbed, fs_path, res, 'tree_extents_scrubbed')
    parse_result_and_set_gauge(
        scrub_data_bytes_scrubbed, fs_path, res, 'data_bytes_scrubbed')
    parse_result_and_set_gauge(
        scrub_tree_bytes_scrubbed, fs_path, res, 'tree_bytes_scrubbed')
    parse_result_and_set_gauge(scrub_read_errors, fs_path, res, 'read_errors')
    parse_result_and_set_gauge(scrub_csum_errors, fs_path, res, 'csum_errors')
    parse_result_and_set_gauge(
        scrub_verify_errors, fs_path, res, 'verify_errors')
    parse_result_and_set_gauge(scrub_no_csum, fs_path, res, 'no_csum')
    parse_result_and_set_gauge(
        scrub_csum_discards, fs_path, res, 'csum_discards')
    parse_result_and_set_gauge(
        scrub_super_errors, fs_path, res, 'super_errors')
    parse_result_and_set_gauge(
        scrub_malloc_errors, fs_path, res, 'malloc_errors')
    parse_result_and_set_gauge(
        scrub_uncorrectable_errors, fs_path, res, 'uncorrectable_errors')
    parse_result_and_set_gauge(
        scrub_unverified_errors, fs_path, res, 'unverified_errors')
    parse_result_and_set_gauge(
        scrub_corrected_errors, fs_path, res, 'corrected_errors')
    parse_result_and_set_gauge(
        scrub_last_physical, fs_path, res, 'last_physical')
    # print(res)
    if re.search('finished', res):
        scrub_status.labels(filesystem_path=fs_path).set(1)
    elif re.search('running', res):
        scrub_status.labels(filesystem_path=fs_path).set(2)
    else:
        scrub_status.labels(filesystem_path=fs_path).set(0)

    pass


if __name__ == "__main__":

    start_http_server(9111)
    while True:
        set_progress()
        time.sleep(10)
