from playwright.sync_api import sync_playwright
import time

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        url = "https://projectmx-users.streamlit.app/"
        
        print(f"Connecting to {url} ...")
        page.goto(url)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except:
            print("Network idle timeout, but continuing...")

        print("Page loaded. Waiting for 30 seconds to simulate activity...")
        
        time.sleep(30)
        
        # page.screenshot(path="screenshot.png")
        
        print("Done. Closing browser.")
        browser.close()

if __name__ == "__main__":
    run()
