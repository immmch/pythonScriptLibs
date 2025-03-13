import time


def timer(func):
    def wrapper():
        start = time.time()
        func()
        end = time.time()
        print(f"执行时间: {end - start:.4f} 秒")

    return wrapper
