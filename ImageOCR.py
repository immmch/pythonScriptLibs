import cv2
import pytesseract
import numpy as np
from matplotlib import pyplot as plt
import time


# 定义装饰器
def timer(func):
    def wrapper():
        start = time.time()
        func()
        end = time.time()
        print(f"执行时间: {end - start:.4f} 秒")

    return wrapper


@timer
def OCR():
    # 说明：定位用户输入的字符在图片中的位置
    # cv2 需要执行：pip install opencv-python 来安装
    # pytesseract 需要安装一个外部软件，在
    #   这个错误 “tesseract is not installed or it's not in your PATH” 说明 Tesseract-OCR 没有安装，或者 Python 找不到它的位置。解决方案如下：
    #   到github地址下载安装：https://github.com/UB-Mannheim/tesseract/wiki，安装时选择中文和添加到path。
    #   安装完成， add path后，需要重启ide，以获取环境变量配置。
    # 读取用户上传的图片
    image_path = "img.png"
    image = cv2.imread(image_path)

    # 将图片转换为灰度图
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # 使用OCR识别文本
    custom_oem_psm_config = r'--oem 3 --psm 6'
    data = pytesseract.image_to_data(gray, lang='chi_sim+eng', config=custom_oem_psm_config,
                                     output_type=pytesseract.Output.DICT)

    # 要查找的文本
    search_text = "手动"

    # 查找匹配文本的位置
    positions = []
    for i, text in enumerate(data['text']):
        if search_text in text:
            x, y, w, h = data['left'][i], data['top'][i], data['width'][i], data['height'][i]
            positions.append((x, y, w, h))

    # 在图片上标记找到的文本位置
    for (x, y, w, h) in positions:
        cv2.rectangle(image, (x, y), (x + w, y + h), (0, 255, 0), 2)

    # 显示标记后的图片
    plt.figure(figsize=(10, 8))
    plt.imshow(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
    plt.axis("off")
    plt.show()

    # 显示原始大小的标记图片
    # plt.figure(figsize=(image.shape[1] / 100, image.shape[0] / 100), dpi=100)
    # plt.imshow(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
    # plt.axis("off")
    # plt.show()

    # 输出找到的坐标位置
    print(positions)
