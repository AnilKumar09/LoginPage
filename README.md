

from selenium import webdriver
def lanuch():
    try:
        driver = webdriver.Chrome(executable_path='Users/sk/Repos/chromedriver.exe')
        driver.get("https://google.com")
    except Exception as e:
        print(e)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    lanuch()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
