import argparse
import os
import time
import re
import unicodedata
import json
import asyncio
import random
from playwright.async_api import async_playwright

MOBILE_UA = "Mozilla/5.0 (Linux; Android 13; vivo V60) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36"

MOBILE_VIEWPORT = {"width": 412, "height": 915}  # Typical Android phone size

LAUNCH_ARGS = [
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-sync",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--mute-audio",
]

def sanitize_input(raw):
    """
    Fix shell-truncated input (e.g., when '&' breaks in CMD or bot execution).
    If input comes as a list (from nargs='+'), join it back into a single string.
    """
    if isinstance(raw, list):
        raw = " ".join(raw)
    return raw

def parse_messages(names_arg):
    """
    Robust parser for messages:
    - If names_arg is a .txt file, first try JSON-lines parsing (one JSON string per line, supporting multi-line messages).
    - If that fails, read the entire file content as a single block and split only on explicit separators '&' or 'and' (preserving newlines within each message for ASCII art).
    - For direct string input, treat as single block and split only on separators.
    This ensures ASCII art (multi-line blocks without separators) is preserved as a single message.
    """
    # Handle argparse nargs possibly producing a list
    if isinstance(names_arg, list):
        names_arg = " ".join(names_arg)

    content = None  
    is_file = isinstance(names_arg, str) and names_arg.endswith('.txt') and os.path.exists(names_arg)  

    if is_file:  
        # Try JSON-lines first (each line is a JSON-encoded string, possibly with \n for multi-line)  
        try:  
            msgs = []  
            with open(names_arg, 'r', encoding='utf-8') as f:  
                lines = [ln.rstrip('\n') for ln in f if ln.strip()]  # Skip empty lines  
            for ln in lines:  
                m = json.loads(ln)  
                if isinstance(m, str):  
                    msgs.append(m)  
                else:  
                    raise ValueError("JSON line is not a string")  
            if msgs:  
                # Normalize each message (preserve \n for art)  
                out = []  
                for m in msgs:  
                    #m = unicodedata.normalize("NFKC", m)  
                    #m = re.sub(r'[\u200B-\u200F\uFEFF\u202A-\u202E\u2060-\u206F]', '', m)  
                    out.append(m)  
                return out  
        except Exception:  
            pass  # Fall through to block parsing on any error  

        # Fallback: read entire file as one block for separator-based splitting  
        try:  
            with open(names_arg, 'r', encoding='utf-8') as f:  
                content = f.read()  
        except Exception as e:  
            raise ValueError(f"Failed to read file {names_arg}: {e}")  
    else:  
        # Direct string input  
        content = str(names_arg)  

    if content is None:  
        raise ValueError("No valid content to parse")  

    # Normalize content (preserve \n for ASCII art)  
    #content = unicodedata.normalize("NFKC", content)  
    #content = content.replace("\r\n", "\n").replace("\r", "\n")  
    #content = re.sub(r'[\u200B-\u200F\uFEFF\u202A-\u202E\u2060-\u206F]', '', content)  

    # Normalize ampersand-like characters to '&' for consistent splitting  
    content = (  
        content.replace('﹠', '&')  
        .replace('＆', '&')  
        .replace('⅋', '&')  
        .replace('ꓸ', '&')  
        .replace('︔', '&')  
    )  

    # Split only on explicit separators: '&' or the word 'and' (case-insensitive, with optional whitespace)  
    # This preserves multi-line blocks like ASCII art unless explicitly separated  
    pattern = r'\s*(?:&|\band\b)\s*'  
    parts = [part.strip() for part in re.split(pattern, content, flags=re.IGNORECASE) if part.strip()]  
    return parts

async def login(args, storage_path, headless):
    """
    Async login function to handle initial Instagram login and save storage state.
    """
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
                args=LAUNCH_ARGS
            )
            context = await browser.new_context(
                user_agent=MOBILE_UA,
                viewport=MOBILE_VIEWPORT,
                is_mobile=True,
                has_touch=True,
                device_scale_factor=2,
                color_scheme="dark"
            )
            page = await context.new_page()
            try:
                print("Logging in to Instagram...")
                await page.goto("https://www.instagram.com/", timeout=60000)
                await page.wait_for_selector('input[name="username"]', timeout=30000)
                await page.fill('input[name="username"]', args.username)
                await page.fill('input[name="password"]', args.password)
                await page.click('button[type="submit"]')
                # Wait for successful redirect (adjust if needed for 2FA or errors)
                await page.wait_for_url("**/home**", timeout=60000)  # More specific to profile/home
                print("Login successful, saving storage state.")
                await context.storage_state(path=storage_path)
                return True
            except Exception as e:
                print(f"Login error: {e}")
                return False
            finally:
                await browser.close()
    except Exception as e:
        print(f"Unexpected login error: {e}")
        return False

async def init_page(page, url, dm_selector):
    """
    Initialize a single page by navigating to the URL with retries.
    Returns True if successful, False otherwise.
    """
    init_success = False
    for init_try in range(3):
        try:
            await page.goto("https://www.instagram.com/", timeout=60000)
            await page.goto(url, timeout=60000)
            await page.wait_for_selector(dm_selector, timeout=30000)
            init_success = True
            break
        except Exception as init_e:
            print(f"Tab for {url[:30]}... try {init_try+1}/3 failed: {init_e}")
            if init_try < 2:
                await asyncio.sleep(2)
    return init_success

async def sender(tab_id, args, messages, context, page):
    """
    Ultra-fast async sender coroutine: Cycles through messages in an infinite loop, preloading/reloading pages every 60s to avoid issues.
    Preserves newlines in messages for multi-line content like ASCII art.
    Uses shared context to create new pages for reloading.
    Enhanced with retry logic: If selector not visible or send fails, retry up to 2 times (press Enter to clear if stuck, then refill), skip if all retries fail, never crash.
    """
    dm_selector = 'div[role="textbox"][aria-label="Message"]'
    print(f"🚀 Tab {tab_id} ready, starting ULTRA FAST infinite message loop.")
    current_page = page
    cycle_start = time.time()
    msg_index = 0
    sent_count = 0
    start_time = time.time()
    while True:
        elapsed = time.time() - cycle_start
        if elapsed >= 120:
            try:
                print(f"🔄 Tab {tab_id} reloading thread after {elapsed:.1f}s")
                # Same URL ka hard reload, kahin aur nahi jayega
                await current_page.reload(timeout=60000)
                await current_page.wait_for_selector(dm_selector, timeout=30000)
            except Exception as reload_e:
                print(f"❌ Tab {tab_id} reload failed after {elapsed:.1f}s: {reload_e}")
                raise Exception(f"Tab {tab_id} reload failed: {reload_e}")
            cycle_start = time.time()
            continue
        msg = messages[msg_index]
        current_url = current_page.url
        print(f"Tab {tab_id} current URL: {current_url[:80]}...")
        send_success = False
        max_retries = 2
        for retry in range(max_retries):
            try:
                locator = current_page.locator(dm_selector)
                count = await locator.count()
                print(f"🔍 Tab {tab_id} found {count} elements with selector '{dm_selector}'")
                
                if count == 0:
                    print(f"⚠️ Tab {tab_id} CRITICAL: No message input found! Page may not be loaded properly")
                    # Try alternative selectors
                    alt_selectors = [
                        'textarea[placeholder="Message..."]',
                        'div[data-testid="message-input"]',
                        'div[contenteditable="true"][role="textbox"]',
                        'div[aria-label="Message"]',
                        'input[placeholder*="Message"]'
                    ]
                    for alt_sel in alt_selectors:
                        alt_locator = current_page.locator(alt_sel)
                        alt_count = await alt_locator.count()
                        if alt_count > 0:
                            print(f"✅ Tab {tab_id} found {alt_count} elements with alternative selector '{alt_sel}'")
                            locator = alt_locator
                            break
                    else:
                        print(f"🚫 Tab {tab_id} FATAL: No message input found with any selector!")
                        raise Exception(f"Tab {tab_id} cannot find message input on page")
                
                if not await locator.is_visible():
                    print(f"Tab {tab_id} selector not visible on retry {retry+1}/{max_retries} for '{msg[:50]}...', attempting Enter to clear.")
                    try:
                        await current_page.press(dm_selector, 'Enter')
                        await asyncio.sleep(0.05)
                    except:
                        pass  # Ignore clear failure
                    await asyncio.sleep(0.02)  # Fast update wait
                    continue  # Retry visibility check

                await locator.click()
                # DO NOT replace \n with space: Preserve multi-line for ASCII art
                # Instagram DM supports multi-line messages via fill()
                await locator.fill(msg)
                await asyncio.sleep(0.05)  # Fast processing delay
                
                # Try to find and click send button first
                send_button_selectors = [
                    'button[type="submit"]',
                    'button[aria-label="Send"]',
                    'button[data-testid="send-button"]',
                    'svg[aria-label="Send"]',
                    'div[role="button"][aria-label*="Send"]'
                ]
                send_button_found = False
                for btn_sel in send_button_selectors:
                    try:
                        btn_locator = current_page.locator(btn_sel)
                        if await btn_locator.count() > 0 and await btn_locator.is_visible():
                            await btn_locator.click()
                            print(f"📤 Tab {tab_id} clicked send button with selector '{btn_sel}'")
                            send_button_found = True
                            break
                    except Exception as btn_e:
                        print(f"🔘 Tab {tab_id} send button error with '{btn_sel}': {btn_e}")
                        continue

                if not send_button_found:
                    print(f"🚫 Tab {tab_id} WARNING: No send button found, using Enter key fallback")
                    # Fallback to Enter key
                    await locator.press('Enter')
                    print(f"⌨️ Tab {tab_id} pressed Enter (no send button found)")
                
                print(f"✅ Tab {tab_id} sent message {msg_index + 1}/{len(messages)} on retry {retry+1}: '{msg[:50]}{'...' if len(msg) > 50 else ''}'")
                
                # Verify message was sent by checking if it appears in chat
                await asyncio.sleep(0.3)  # Fast verification wait
                try:
                    # Look for the message text in recent messages
                    message_selectors = [
                        f'div[role="listitem"]:has-text("{msg[:20]}")',
                        f'span:has-text("{msg[:20]}")',
                        'div[data-testid="message-item"]',
                        'div[role="listitem"]'
                    ]
                    message_found = False
                    for msg_sel in message_selectors:
                        msg_elements = current_page.locator(msg_sel)
                        count = await msg_elements.count()
                        if count > 0:
                            # Check if any contain our message text
                            for i in range(min(count, 5)):  # Check last 5 messages
                                try:
                                    text = await msg_elements.nth(i).inner_text()
                                    if msg[:20].lower() in text.lower():
                                        message_found = True
                                        print(f"✅ Tab {tab_id} verified message sent successfully")
                                        break
                                except:
                                    continue
                        if message_found:
                            break
                    
                    if not message_found:
                        print(f"🚫 Tab {tab_id} message verification failed, Instagram may have blocked it")
                        # Don't raise exception, just log and continue with longer delay
                        await asyncio.sleep(random.uniform(8.0, 15.0))  # Longer delay when blocked
                        
                except Exception as verify_e:
                    verify_error = str(verify_e).lower()
                    if "timeout" in verify_error:
                        print(f"⏰ Tab {tab_id} VERIFICATION TIMEOUT: Chat didn't load fast enough")
                    elif "element" in verify_error:
                        print(f"🔍 Tab {tab_id} VERIFICATION ERROR: Chat elements not found")
                    else:
                        print(f"🔍 Tab {tab_id} VERIFICATION ERROR: {verify_e}")
                
                send_success = True
                sent_count += 1
                if sent_count % 10 == 0:  # Status update every 10 messages
                    elapsed_total = time.time() - start_time
                    msg_per_min = (sent_count / elapsed_total) * 60
                    print(f"🎯 Tab {tab_id} MILESTONE: {sent_count} messages sent | {msg_per_min:.1f} msg/min")
                break
            except Exception as send_e:
                error_msg = str(send_e).lower()
                if "target page" in error_msg or "context" in error_msg or "browser" in error_msg:
                    print(f"💥 Tab {tab_id} CRITICAL ERROR: Browser crashed/disconnected - {send_e}")
                    raise Exception(f"Tab {tab_id} browser error: {send_e}")
                elif "timeout" in error_msg:
                    print(f"⏰ Tab {tab_id} TIMEOUT ERROR: Element not found within time limit - {send_e}")
                elif "element" in error_msg or "selector" in error_msg:
                    print(f"🎯 Tab {tab_id} ELEMENT ERROR: Message input not found - {send_e}")
                elif "network" in error_msg or "connection" in error_msg:
                    print(f"🌐 Tab {tab_id} NETWORK ERROR: Connection issue - {send_e}")
                else:
                    print(f"❌ Tab {tab_id} UNKNOWN ERROR on retry {retry+1}/{max_retries}: {send_e}")
                if retry < max_retries - 1:
                    print(f"🔄 Tab {tab_id} retrying after brief pause...")
                    await asyncio.sleep(0.05)
                else:
                    print(f"💀 Tab {tab_id} all retries failed, triggering restart.")
        if not send_success:
            raise Exception(f"Tab {tab_id} failed to send after {max_retries} retries")
        await asyncio.sleep(random.uniform(0.5, 2.0))  # Ultra fast random delay between sends (0.5-2 seconds)
        msg_index = (msg_index + 1) % len(messages)

async def main():
    parser = argparse.ArgumentParser(description="Instagram DM Auto Sender using Playwright")
    parser.add_argument('--username', required=False, help='Instagram username (required for initial login)')
    parser.add_argument('--password', required=False, help='Instagram password (required for initial login)')
    parser.add_argument('--thread-url', required=True, help='Full Instagram direct thread URLs (comma-separated for multiple)')
    parser.add_argument('--names', nargs='+', required=True, help='Messages list, direct string, or .txt file (split on & or "and" for multiple; preserves newlines for art)')
    parser.add_argument('--headless', default='true', choices=['true', 'false'], help='Run in headless mode (default: true)')
    parser.add_argument('--storage-state', required=True, help='Path to JSON file for login state (persists session)')
    parser.add_argument('--tabs', type=int, default=10, help='Number of parallel tabs per thread URL (1-15, default 10)')
    args = parser.parse_args()
    args.names = sanitize_input(args.names)  # Handle bot/shell-truncated inputs

    thread_urls = [u.strip() for u in args.thread_url.split(',') if u.strip()]
    if not thread_urls:
        print("Error: No valid thread URLs provided.")
        return

    headless = args.headless == 'true'  
    storage_path = args.storage_state  
    do_login = not os.path.exists(storage_path)  

    if do_login:  
        if not args.username or not args.password:  
            print("Error: Username and password required for initial login.")  
            return  
        success = await login(args, storage_path, headless)
        if not success:
            return
    else:  
        print("Using existing storage state, skipping login.")  

    try:  
        messages = parse_messages(args.names)  
    except ValueError as e:  
        print(f"Error parsing messages: {e}")  
        return  

    if not messages:  
        print("Error: No valid messages provided.")  
        return  

    print(f"Parsed {len(messages)} messages.")  

    tabs = min(max(args.tabs, 1), 5)  
    total_tabs = len(thread_urls) * tabs
    print(f"Using {tabs} tabs per URL across {len(thread_urls)} URLs (total: {total_tabs} tabs).")  

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=LAUNCH_ARGS
        )
        context = await browser.new_context(
            storage_state=storage_path,
            user_agent=MOBILE_UA,
            viewport=MOBILE_VIEWPORT,
            is_mobile=True,
            has_touch=True,
            device_scale_factor=2,
            color_scheme="dark"
        )
        dm_selector = 'div[role="textbox"][aria-label="Message"]'
        pages = []
        tasks = []
        try:
            while True:
                # Close previous pages and cancel tasks if any
                for page in pages:
                    try:
                        await page.close()
                    except Exception:
                        pass
                pages = []
                for task in tasks:
                    try:
                        task.cancel()
                    except Exception:
                        pass
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []

                # Create all pages first
                page_urls = []
                for url in thread_urls:
                    for i in range(tabs):
                        page = await context.new_page()
                        page_urls.append((page, url))

                # Initialize all pages concurrently
                init_tasks = [asyncio.create_task(init_page(page, url, dm_selector)) for page, url in page_urls]
                init_results = await asyncio.gather(*init_tasks, return_exceptions=True)

                # Filter successful initializations
                for idx, result in enumerate(init_results):
                    page, url = page_urls[idx]
                    if isinstance(result, Exception) or not result:
                        print(f"Tab for {url} failed to initialize after 3 tries, skipping.")
                        try:
                            await page.close()
                        except:
                            pass
                    else:
                        pages.append(page)
                        print(f"Tab {len(pages)} ready for {url[:50]}...")

                if not pages:
                    print("No tabs could be initialized, exiting.")
                    return

                actual_tabs = len(pages)
                tasks = [asyncio.create_task(sender(j + 1, args, messages, context, pages[j])) for j in range(actual_tabs)]
                print(f"Starting {actual_tabs} tab(s) in infinite message loop. Press Ctrl+C to stop.")

                pending = set(tasks)
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        if task.exception():
                            exc = task.exception()
                            print(f"Tab task raised exception: {exc}")
                            # Cancel remaining tasks
                            for t in list(pending):
                                t.cancel()
                            await asyncio.gather(*pending, return_exceptions=True)
                            pending.clear()
                            break
                    else:
                        continue
                    break  # If we broke due to exception, exit inner while
        except KeyboardInterrupt:
            print("\nStopping all tabs...")
        finally:
            for page in pages:
                try:
                    await page.close()
                except Exception:
                    pass
            await context.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())