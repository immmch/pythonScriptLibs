import mss
import ImageOCR



@ImageOCR.timer
def screenshot():
    with mss.mss() as sct:
        screenshot = sct.shot(output="screenshot.png")  # 直接保存



screenshot()