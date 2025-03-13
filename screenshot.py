import mss
import elapsedTime


@elapsedTime.timer
def screenshot():
    with mss.mss() as sct:
        screenshot = sct.shot(output="screenshot.png")  # 直接保存


if __name__ == '__main__':
    screenshot()
