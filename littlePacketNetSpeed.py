import socket
import threading
import time
import argparse
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

# 流量测试程序
#   可能缩进还有问题
#   执行命令：python3 <FILE_NAME>.py --client 172.16.20.1 -z 64 -d 100 --tcp
#   更详细的命令使用需要自己看代码了。
# 注意软路由一般对大包友好，小包在硬件层面没有特定的优化。
#
def send_small_packet_data(host, port, data, packet_size, test_duration, packet_rates):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    start_time = time.time()
    packet_count = 0
    second_count = 0
    packet_rate = 0
    while time.time() - start_time < test_duration:
        try:
            sock.sendto(data.encode(), (host, port))
            packet_count += 1
            elapsed_time = time.time() - start_time
            if int(elapsed_time) > second_count:
                packet_rate = packet_count
                second_count += 1
                packet_rates.append(packet_rate)
                human_readable_rate = get_human_readable_packet_rate(packet_rate)
                transmission_rate = packet_rate * packet_size * 8  # 以比特/秒为单位
                human_readable_transmission_rate = get_human_readable_transmission_rate(transmission_rate)
                print(
                    f"Sent {packet_rate} packets in last second, current rate: {human_readable_rate}, packet size: {len(data)} bytes, transmission rate: {human_readable_transmission_rate}")
                packet_count = 0
        except socket.error as e:
            print(f"Error sending data: {e}")
            break
    print(f"Finished sending packets for {second_count} seconds.")
    sock.close()


def get_human_readable_packet_rate(packet_rate):
    if packet_rate >= 10 ** 9:
        return f"{packet_rate / 10 ** 9:.2f} gpps"
    elif packet_rate >= 10 ** 6:
        return f"{packet_rate / 10 ** 6:.2f} mpps"
    elif packet_rate >= 10 ** 3:
        return f"{packet_rate / 10 ** 3:.2f} kpps"
    else:
        return f"{packet_rate:.2f} pps"


def get_human_readable_transmission_rate(transmission_rate):
    units = ["bps", "Kbps", "Mbps", "Gbps"]
    unit_index = 0
    while transmission_rate >= 1000 and unit_index < 3:
        transmission_rate /= 1000
        unit_index += 1
    return f"{transmission_rate:.2f} {units[unit_index]}"


def receive_small_packet_data(host, port, buffer_size, packet_rates):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    start_time = time.time()
    packet_count = 0
    second_count = 0
    packet_rate = 0
    try:
        while True:
            data, _ = sock.recvfrom(buffer_size)
            packet_count += 1
            elapsed_time = time.time() - start_time
            if int(elapsed_time) > second_count:
                packet_rate = packet_count
                second_count += 1
                packet_rates.append(packet_rate)
                human_readable_rate = get_human_readable_packet_rate(packet_rate)
                print(
                    f"Received {packet_count} packets in last second, current rate: {human_readable_rate}, packet size: {len(data)} bytes")
                packet_count = 0
    except KeyboardInterrupt:
        print(f"Stopped receiving packets. Total received: {packet_count} packets")
        sock.close()


def generate_packet_rate_chart(packet_rates, host, port, packet_size, test_duration):
    plt.plot(packet_rates)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Packet Rate (packets/second)')
    plt.xticks(np.arange(0, test_duration + 1, step=max(1, test_duration // 10)))
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    plt.title(f"Packet Rate Test - {timestamp}", fontsize=10)
    test_info = f"Test Parameters:n Host: {host} Port: {port} Packet Size: {packet_size} bytes Test Duration: {test_duration} seconds"
    plt.figtext(0.5, -0.05, test_info, wrap=True, horizontalalignment='center', fontsize=10)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"packet_rate_{timestamp}.jpeg")
    plt.show()


def send_tcp_data(host, port, data, packet_size, test_duration, packet_rates):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    start_time = time.time()
    packet_count = 0
    second_count = 0
    packet_rate = 0
    while time.time() - start_time < test_duration:
        try:
            sock.send(data.encode())
            packet_count += 1
            elapsed_time = time.time() - start_time
            if int(elapsed_time) > second_count:
                packet_rate = packet_count
                second_count += 1
                packet_rates.append(packet_rate)
                human_readable_rate = get_human_readable_packet_rate(packet_rate)
                transmission_rate = packet_rate * packet_size * 8  # 以比特/秒为单位
                human_readable_transmission_rate = get_human_readable_transmission_rate(transmission_rate)
                print(
                    f"Sent {packet_rate} packets in last second, current rate: {human_readable_rate}, packet size: {len(data)} bytes, transmission rate: {human_readable_transmission_rate}")
                packet_count = 0
        except socket.error as e:
            print(f"Error sending data: {e}")
            break
    print(f"Finished sending packets for {second_count} seconds.")
    sock.close()


def receive_tcp_data(host, port, buffer_size, packet_rates):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(1)
    conn, addr = sock.accept()
    start_time = time.time()
    packet_count = 0
    second_count = 0
    packet_rate = 0
    try:
        while True:
            data = conn.recv(buffer_size)
            if not data:
                break
                packet_count += 1
                elapsed_time = time.time() - start_time
                if int(elapsed_time) > second_count:
                    packet_rate = packet_count
                    second_count += 1
                    packet_rates.append(packet_rate)
                    human_readable_rate = get_human_readable_packet_rate(packet_rate)
                    print(
                        f"Received {packet_count} packets in last second, current rate: {human_readable_rate}, packet size: {len(data)} bytes")
                    packet_count = 0
    except KeyboardInterrupt:
        print(f"Stopped receiving packets. Total received: {packet_count} packets")
        sock.close()


def run_test(server_mode, tcp_mode, host, port, data, packet_size, test_duration):
    packet_rates = []
    if tcp_mode:
        send_func = send_tcp_data
        receive_func = receive_tcp_data
    else:
        send_func = send_small_packet_data
        receive_func = receive_small_packet_data
    if server_mode:
        print("Running in server mode...")
        receive_func(host, port, packet_size, packet_rates)
    else:
        print("Running in client mode...")
        start_time = time.time()
        thread = threading.Thread(target=send_func,
                                  args=(host, port, data, packet_size, test_duration, packet_rates))
        thread.start()
        thread.join()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Sent packets for {elapsed_time:.4f} seconds.")
        generate_packet_rate_chart(packet_rates, host, port, packet_size, test_duration)
    if __name__ == "__main__":
        parser = argparse.ArgumentParser(description="Small Packet Data Test")
        parser.add_argument("--server", action="store_true", help="Run in server mode")
        parser.add_argument("--client", action="store_true", help="Run in client mode")
        parser.add_argument("--tcp", action="store_true", help="Run in TCP mode")
        parser.add_argument("host", type=str, help="Server IP address")
        parser.add_argument("-z", "--packet-size", type=int, default=64, help="Packet size in bytes (default: 64)")
        parser.add_argument("-d", "--test-duration", type=float, default=10.0,
                            help="Test duration in seconds (client mode only, default: 10.0)")
        args = parser.parse_args()
        host = args.host
        port = 12345
        data = 'X' * args.packet_size
        packet_size = args.packet_size
        test_duration = args.test_duration
    if args.server:
        run_test(True, args.tcp, host, port, data, packet_size, None)
    elif args.client:
        run_test(False, args.tcp, host, port, data, packet_size, test_duration)
    else:
        print("Please specify either server mode or client mode.")
