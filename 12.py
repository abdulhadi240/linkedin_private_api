async def process_all_batches_concurrent(accounts: List[Dict], batches: List[List[str]]) -> List[Dict]:
    """Processes all batches concurrently with controlled concurrency."""
    num_to_process = min(len(accounts), len(batches))
    logger.info(f"Starting concurrent processing of {num_to_process} batches")
    logger.info(f"Concurrency settings: max_concurrent={CONFIG['concurrency']['max_concurrent_batches']}, "
               f"start_delay={CONFIG['concurrency']['batch_start_delay_range']}s")
    
    # Create semaphore to limit concurrent batches
    semaphore = asyncio.Semaphore(CONFIG["concurrency"]["max_concurrent_batches"])
    
    # Create HTTP session with connection pooling
    connector = aiohttp.TCPConnector(
        limit=20,  # Total connection pool size
        limit_per_host=5,  # Max connections per host
        ttl_dns_cache=300,  # DNS cache TTL
        use_dns_cache=True,
    )
    
    timeout = aiohttp.ClientTimeout(total=CONFIG["concurrency"]["request_timeout"])
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Create tasks for all batches
        tasks = []
        for i in range(num_to_process):
            account = accounts[i]
            batch_profile_ids = batches[i]
            
            task = process_batch(session, account, batch_profile_ids, i + 1, semaphore)
            tasks.append(task)
        
        # Wait for all batches to complete
        logger.info("All batches started. Waiting for completion...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        all_results = []
        successful_batches = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Batch {i + 1} raised an exception: {result}")
            elif isinstance(result, list) and result:
                all_results.extend(result)
                successful_batches += 1
                logger.info(f"Batch {i + 1} completed with {len(result)} results")
            else:
                logger.warning(f"Batch {i + 1} completed with no results")
        
        logger.info(f"Concurrent processing completed: {successful_batches}/{num_to_process} batches successful")
        logger.info(f"Total results collected: {len(all_results)}")
        
        return all_results

def run_concurrent_scraping(accounts: List[Dict], profile_ids: List[str]) -> List[Dict]:
    """Main function to run concurrent scraping with proper event loop handling."""
    # Split profile IDs into batches of 50
    batches = split_batches(profile_ids, 50)
    
    # Run the async processing
    try:
        # Check if we're already in an event loop (e.g., in Jupyter)
        loop = asyncio.get_running_loop()
        logger.warning("Already in an event loop. Using nest_asyncio might be needed.")
        # If we're in a running loop, we need to handle this differently
        import nest_asyncio
        nest_asyncio.apply()
        return asyncio.run(process_all_batches_concurrent(accounts, batches))
    except RuntimeError:
        # No event loop running, safe to use asyncio.run
        return asyncio.run(process_all_batches_concurrent(accounts, batches))

# === MAIN EXECUTION ===
if __name__ == "__main__":
    logger.info("--- Starting LinkedIn Scraping Process (Concurrent Mode) ---")

    # Initialize the client once and pass it to functions
    gspread_client = get_gsheet_client()
    if not gspread_client:
        logger.error("Execution stopped: Could not authorize with Google Sheets.")
        exit()

    accounts = read_accounts(gspread_client)
    profile_ids = read_profile_urls(gspread_client)

    if not accounts:
        logger.error("Execution stopped: No verified accounts were found.")
        exit()
    
    if not profile_ids:
        logger.error("Execution stopped: No profile identifiers were found to process.")
        exit()

    logger.info(f"Loaded {len(accounts)} accounts and {len(profile_ids)} profile IDs")
    
    # Run concurrent scraping
    all_results = run_concurrent_scraping(accounts, profile_ids)

    # Save results to CSV and Google Sheets
    if all_results:
        # Write to CSV file (NEW)
        csv_filepath = write_results_to_csv(all_results)
        if csv_filepath:
            logger.info(f"✅ All scraped data saved to: {csv_filepath}")
        
        # Also write to Google Sheets (original functionality)
        write_results(gspread_client, all_results)
        logger.info(f"Successfully completed scraping with {len(all_results)} total results")
    else:
        logger.warning("No data was collected from any batch.")