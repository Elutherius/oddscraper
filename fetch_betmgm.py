from playwright.sync_api import sync_playwright
import time
import os

def scrape_betmgm_structure():
    # URL - Try Canada (Ontario) which might be the user's region and have different protection
    url = "https://sports.betmgm.ca/en/sports"
    
    os.makedirs("betmgm_data", exist_ok=True)
    
    with sync_playwright() as p:
        # Launch options
        browser = p.chromium.launch(
            headless=False,
            channel="chrome", 
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized"
            ]
        )
        
        context = browser.new_context(
             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
             viewport={'width': 1920, 'height': 1080}
        )
        
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = context.new_page()
        
        print(f"Navigating to {url}...")
        try:
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # Wait and Interact
            print("Page loaded. Sleeping 5s before interaction...")
            time.sleep(5)
            
            # Simulate mouse movement
            page.mouse.move(100, 100)
            page.mouse.down()
            page.mouse.up()
            time.sleep(2)
            page.mouse.move(200, 200)
            
            # Check for title again
            print(f"Page Title: {page.title()}")
            
            # Try to click on a sport if visible
            try:
                # Look for "NBA" or "Basketball"
                page.click("text=NBA", timeout=5000)
                print("Clicked NBA!")
                time.sleep(5)
            except:
                print("Could not click NBA link.")

            # Save HTML for inspection
            content = page.content()
            with open("betmgm_data/page_dump.html", "w", encoding="utf-8") as f:
                f.write(content)
            print("Saved HTML dump to betmgm_data/page_dump.html")
            
            # Save screenshot
            page.screenshot(path="betmgm_data/page_view.png")
            print("Saved screenshot to betmgm_data/page_view.png")
            
        except Exception as e:
            print(f"Error: {e}")
            
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_betmgm_structure()
