"""
Analyzer Tools browser automation using Playwright.

Automates the analyzer.tools workflow:
1. Login with credentials
2. Upload ASIN CSV for processing
3. Wait for analysis to complete
4. Download Excel results

Environment variables:
    ANALYZER_USERNAME: Login username
    ANALYZER_PASSWORD: Login password
    ANALYZER_DOWNLOAD_PATH: Directory for downloads (optional, uses temp dir if not set)
"""

import os
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional
import tempfile

from playwright.async_api import async_playwright, Page, Browser


# Configuration from environment
# Support both old env vars (username/password) and new explicit ones (ANALYZER_USERNAME/ANALYZER_PASSWORD)
ANALYZER_USERNAME = os.getenv("ANALYZER_USERNAME") or os.getenv("username", "")
ANALYZER_PASSWORD = os.getenv("ANALYZER_PASSWORD") or os.getenv("password", "")
ANALYZER_DOWNLOAD_PATH = os.getenv("ANALYZER_DOWNLOAD_PATH", "")

ANALYZER_URL = "https://app.analyzer.tools/#/sign-in"


async def login(page: Page) -> bool:
    """
    Login to analyzer.tools.
    
    Returns:
        True if login successful, False otherwise.
    """
    await page.goto(ANALYZER_URL)
    await page.wait_for_load_state("networkidle")
    
    # Fill credentials
    username_input = page.locator("input[placeholder='Username']")
    await username_input.fill(ANALYZER_USERNAME)
    
    password_input = page.locator("input[placeholder='Password']")
    await password_input.fill(ANALYZER_PASSWORD)
    
    # Click sign in
    login_button = page.locator("button:has-text('Sign In')")
    await login_button.click()
    
    # Wait for dashboard to load
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(3000)  # Extra wait for SPA navigation
    
    print("Login completed")
    return True


async def upload_csv(page: Page, csv_path: str) -> bool:
    """
    Upload CSV file for processing.
    
    Args:
        page: Playwright page object
        csv_path: Absolute path to the CSV file to upload
        
    Returns:
        True if upload started successfully.
    """
    # Click 'New Scan' dropdown
    new_scan_button = page.locator("button:has(span:has-text('New Scan'))")
    await new_scan_button.click()
    await page.wait_for_timeout(1000)
    
    # Click 'Process Spreadsheet'
    process_option = page.locator("span:has-text('Process Spreadsheet')")
    await process_option.click()
    await page.wait_for_timeout(2000)
    
    # Upload file via hidden input
    file_input = page.locator("input[type='file']")
    await file_input.set_input_files(csv_path)
    await page.wait_for_timeout(2000)
    
    print(f"File uploaded: {csv_path}")
    
    # Configure checkboxes based on UI structure from HTML:
    # div.flex.items-center.gap-4 (row container)
    #   div.cursor-pointer (clickable checkbox wrapper) 
    #     input[type=checkbox] (hidden)
    #   span > div (label text like "Email Notification")
    
    # 1. UNCHECK Email Notification (it's checked by default)
    try:
        # Find by the exact text, then navigate to parent container and find checkbox
        email_row = page.locator("div.flex.items-center.gap-4:has(div:text-is('Email Notification'))").first
        if await email_row.count() > 0:
            email_wrapper = email_row.locator("div.cursor-pointer").first
            email_checkbox = email_wrapper.locator("input[type='checkbox']")
            
            if await email_checkbox.count() > 0:
                is_checked = await email_checkbox.is_checked()
                print(f"  Email Notification checkbox state: {'checked' if is_checked else 'unchecked'}")
                if is_checked:
                    await email_wrapper.click()
                    await page.wait_for_timeout(500)
                    print("✓ Email Notification checkbox UNCHECKED")
                else:
                    print("✓ Email Notification already unchecked")
            else:
                print("WARNING: Could not find Email checkbox input element")
        else:
            print("WARNING: Could not find Email Notification row")
    except Exception as e:
        print(f"WARNING: Could not handle Email Notification: {e}")
    
    await page.wait_for_timeout(500)
    
    # 2. CHECK Enable Premium Data
    try:
        # Find by the exact text, then navigate to parent container and find checkbox
        premium_row = page.locator("div.flex.items-center.gap-4:has(div:text-is('Enable Premium Data'))").first
        if await premium_row.count() > 0:
            premium_wrapper = premium_row.locator("div.cursor-pointer").first
            premium_checkbox = premium_wrapper.locator("input[type='checkbox']")
            
            if await premium_checkbox.count() > 0:
                is_checked = await premium_checkbox.is_checked()
                print(f"  Enable Premium Data checkbox state: {'checked' if is_checked else 'unchecked'}")
                if not is_checked:
                    await premium_wrapper.click()
                    await page.wait_for_timeout(500)
                    print("✓ Enable Premium Data checkbox CHECKED")
                else:
                    print("✓ Enable Premium Data already checked")
            else:
                print("WARNING: Could not find Premium Data checkbox input element")
        else:
            print("WARNING: Could not find Enable Premium Data row")
    except Exception as e:
        print(f"WARNING: Could not handle Premium Data: {e}")
    
    await page.wait_for_timeout(1000)
    
    # Click Process button
    process_button = page.locator("button:has-text('Process')")
    await process_button.click()
    await page.wait_for_timeout(3000)
    
    # Click Okay confirmation if present
    try:
        okay_button = page.locator("button:has-text('Okay')")
        if await okay_button.count() > 0:
            await okay_button.click()
            await page.wait_for_timeout(1000)
    except Exception:
        pass
    
    # Click Process again if needed
    try:
        process_button = page.locator("button:has-text('Process')")
        if await process_button.count() > 0:
            await process_button.click()
    except Exception:
        pass
    
    print("Processing started")
    return True


async def wait_for_results(page: Page, upload_id: str, timeout_seconds: int = 300) -> bool:
    """
    Wait for the spreadsheet results to appear for our specific upload.
    
    Args:
        page: Playwright page object
        upload_id: Unique identifier from our uploaded filename (e.g., '20260122_163845')
        timeout_seconds: Maximum time to wait for results (default 5 minutes)
        
    Returns:
        True if results appeared, False if timeout.
    """
    print(f"Waiting up to {timeout_seconds}s for results matching upload_id: {upload_id}...")
    
    # Look specifically for OUR upload by matching the upload_id in the result name
    # The result name includes the uploaded filename, e.g., "Spreadsheet-input-asins_20260122_163845"
    specific_selectors = [
        f"a.file-name:has-text('{upload_id}')",
        f"a[class*='file-name']:has-text('{upload_id}')",
        f"a:has-text('asins_{upload_id}')",
    ]
    
    # Fallback to generic selectors if specific ones don't work
    generic_selectors = [
        "a.file-name:has-text('Spreadsheet')",
        "a[class*='file-name']:has-text('Spreadsheet')",
    ]
    
    try:
        # First, try to find our specific upload result
        for i in range(timeout_seconds // 5):
            # Try specific selectors first (matching our upload_id)
            for selector in specific_selectors:
                try:
                    locator = page.locator(selector)
                    if await locator.count() > 0:
                        print(f"Results ready - found OUR upload (matched: {selector})")
                        return True
                except Exception:
                    pass
            
            # After 60 seconds, also check generic selectors as fallback
            # (in case analyzer.tools changed naming convention)
            if i >= 12:  # 60 seconds
                for selector in generic_selectors:
                    try:
                        locator = page.locator(selector)
                        if await locator.count() > 0:
                            # Check if this is a recent result by looking at the text
                            text = await locator.first.text_content()
                            print(f"Found generic result: {text}")
                            # Accept it but warn
                            print(f"WARNING: Using generic match (couldn't find upload_id {upload_id})")
                            return True
                    except Exception:
                        pass
            
            # Wait 5 seconds before next poll
            await page.wait_for_timeout(5000)
            elapsed = (i + 1) * 5
            if elapsed % 30 == 0:  # Log every 30 seconds
                print(f"  Still waiting for upload_id {upload_id}... ({elapsed}s)")
        
        print(f"Timeout - results not found for upload_id: {upload_id}")
        return False
        
    except Exception as e:
        print(f"Error waiting for results: {e}")
        return False


async def download_excel(page: Page, download_dir: str, upload_id: str) -> Optional[str]:
    """
    Download the Excel export from results.
    
    Args:
        page: Playwright page object
        download_dir: Directory to save the downloaded file
        upload_id: Unique identifier to find our specific result
        
    Returns:
        Path to downloaded file, or None if failed.
    """
    # Click the spreadsheet result link - try specific selectors first, then generic
    result_selectors = [
        f"a.file-name:has-text('{upload_id}')",
        f"a[class*='file-name']:has-text('{upload_id}')",
        "a.file-name:has-text('Spreadsheet')",
        "a[class*='file-name']:has-text('Spreadsheet')",
        "a:has-text('Spreadsheet-input')",
    ]
    
    clicked = False
    for selector in result_selectors:
        try:
            result_link = page.locator(selector).first
            if await result_link.count() > 0:
                await result_link.click()
                clicked = True
                print(f"Clicked result link: {selector}")
                break
        except Exception:
            continue
    
    if not clicked:
        print("Could not find result link to click")
        return None
    
    # Wait 45 seconds for the report to fully load in the frontend
    # This prevents downloading incomplete Excel files
    print("Waiting 45 seconds for report to fully load in browser...")
    await page.wait_for_timeout(45000)
    
    # Open dropdown and select "Full Data View"
    try:
        dropdown_icon = page.locator(".dx-dropdowneditor-icon").first
        await dropdown_icon.click()
        await page.wait_for_timeout(1000)
        
        full_view_option = page.locator("div.dx-item-content:has-text('Full Data View')")
        await full_view_option.click()
        await page.wait_for_timeout(2000)
        print("Selected Full Data View")
    except Exception as e:
        print(f"Could not select Full Data View (may not be needed): {e}")
    
    # Click export dropdown - try multiple selectors
    export_dropdown_selectors = [
        "._DropdownToggle_1eimg_1",
        "[class*='DropdownToggle']",
        "button:has-text('Export')",
        ".dropdown-toggle",
    ]
    
    for selector in export_dropdown_selectors:
        try:
            dropdown = page.locator(selector).first
            if await dropdown.count() > 0:
                await dropdown.click()
                print(f"Clicked export dropdown: {selector}")
                await page.wait_for_timeout(2000)
                break
        except Exception:
            continue
    
    # Click 'Export Excel File' - the original uses XPath with contains text
    # Use evaluate to do JavaScript click like original script
    try:
        # Try the original XPath approach
        export_btn = page.locator("xpath=//span[contains(text(), 'Export Excel File')]")
        if await export_btn.count() > 0:
            await export_btn.evaluate("el => el.click()")
            print("Clicked 'Export Excel File' via JS evaluate")
        else:
            # Fallback to other selectors
            export_button_selectors = [
                "text=Export Excel File",
                "span:has-text('Export Excel')",
            ]
            for selector in export_button_selectors:
                try:
                    btn = page.locator(selector).first
                    if await btn.count() > 0:
                        await btn.evaluate("el => el.click()")
                        print(f"Clicked export button via JS: {selector}")
                        break
                except Exception:
                    continue
    except Exception as e:
        print(f"Error clicking export button: {e}")
        await page.screenshot(path=os.path.join(download_dir, "export_click_error.png"))
        return None
    
    await page.wait_for_timeout(3000)  # Wait for modal to appear
    
    # Take screenshot to see what we have
    await page.screenshot(path=os.path.join(download_dir, "after_export_click.png"))
    print("Screenshot saved: after_export_click.png")
    
    # Enter filename - include upload_id for traceability
    export_filename = f"AnalyzerTools_Export_{upload_id}"
    
    filename_selectors = [
        "#exportFileName",
        "input[id*='exportFileName']",
        "input[name*='fileName']",
        ".modal input[type='text']",
        "input[placeholder*='file']",
    ]
    
    filename_filled = False
    for selector in filename_selectors:
        try:
            filename_input = page.locator(selector).first
            if await filename_input.count() > 0 and await filename_input.is_visible():
                await filename_input.fill(export_filename)
                filename_filled = True
                print(f"Filled filename using: {selector}")
                break
        except Exception as e:
            print(f"Filename selector {selector} failed: {e}")
            continue
    
    if not filename_filled:
        # Maybe no filename input needed, just save directly
        print("No filename input found, trying to save directly")
        await page.screenshot(path=os.path.join(download_dir, "no_filename_input.png"))
    
    await page.wait_for_timeout(500)
    
    # Start download and wait for it
    save_button_selectors = [
        "button.btn-modal:has-text('Save')",
        "button:has-text('Save')",
        "button:has-text('Download')",
        "button:has-text('Export')",
        ".modal button.btn-primary",
    ]
    
    async with page.expect_download(timeout=60000) as download_info:
        for selector in save_button_selectors:
            try:
                save_button = page.locator(selector).first
                if await save_button.count() > 0 and await save_button.is_visible():
                    await save_button.click()
                    print(f"Clicked save button: {selector}")
                    break
            except Exception:
                continue
    
    download = await download_info.value
    
    # Save to specified directory
    filepath = os.path.join(download_dir, f"{export_filename}.xlsx")
    await download.save_as(filepath)
    
    print(f"Downloaded: {filepath}")
    return filepath


async def run_analyzer_workflow(csv_path: str, download_dir: Optional[str] = None, upload_id: Optional[str] = None) -> Optional[str]:
    """
    Run the complete analyzer.tools workflow.
    
    Args:
        csv_path: Path to the ASIN CSV file to upload
        download_dir: Directory for downloads (optional)
        upload_id: Unique identifier for this upload (used to find our specific result)
        
    Returns:
        Path to downloaded Excel file, or None if failed.
    """
    if not ANALYZER_USERNAME or not ANALYZER_PASSWORD:
        raise ValueError("ANALYZER_USERNAME and ANALYZER_PASSWORD must be set in environment")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Extract upload_id from csv filename if not provided
    if not upload_id:
        # Try to extract from filename like "asins_20260122_163845.csv"
        import re
        match = re.search(r'asins_(\d{8}_\d{6})', csv_path)
        upload_id = match.group(1) if match else datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"[Analyzer] Upload ID for this session: {upload_id}")
    
    # Use provided dir, env var, or temp directory
    if not download_dir:
        download_dir = ANALYZER_DOWNLOAD_PATH or tempfile.mkdtemp(prefix="analyzer_")
    
    os.makedirs(download_dir, exist_ok=True)
    
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=UseDnsHttpsSvcb,EnableIPv6",
            ]
        )
        
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1920, "height": 1080}
        )
        
        page = await context.new_page()
        
        try:
            # Step 1: Login
            await login(page)
            
            # Step 2: Upload CSV
            await upload_csv(page, csv_path)
            
            # Step 3: Wait for results (5 min timeout for large CSVs)
            # Pass upload_id so we wait for OUR specific result, not old ones
            if not await wait_for_results(page, upload_id, timeout_seconds=300):
                print("Failed to get results within timeout")
                return None
            
            # Step 3.5: Wait 45 seconds before accessing the report
            # This ensures the report is fully ready on the backend
            print("Waiting 45 seconds before accessing report (allowing full load)...")
            await page.wait_for_timeout(45000)
            
            # Step 4: Download Excel (pass upload_id to find our specific result)
            filepath = await download_excel(page, download_dir, upload_id)
            return filepath
            
        except Exception as e:
            print(f"Analyzer workflow failed: {e}")
            # Take screenshot for debugging
            screenshot_path = os.path.join(download_dir, "error_screenshot.png")
            await page.screenshot(path=screenshot_path)
            print(f"Screenshot saved: {screenshot_path}")
            return None
            
        finally:
            await browser.close()


def run_analyzer_sync(csv_path: str, download_dir: Optional[str] = None, upload_id: Optional[str] = None) -> Optional[str]:
    """
    Synchronous wrapper for run_analyzer_workflow.
    
    Use this from non-async code (e.g., scheduled jobs).
    """
    return asyncio.run(run_analyzer_workflow(csv_path, download_dir, upload_id))


if __name__ == "__main__":
    # Test run
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python analyzer_scraper.py <csv_path>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    result = run_analyzer_sync(csv_path)
    
    if result:
        print(f"Success! Excel file: {result}")
    else:
        print("Failed to complete analyzer workflow")
        sys.exit(1)
