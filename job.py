#!/usr/bin/env python3
"""
LeadFlowX Nightly Scoring Job
Enhanced with error handling, idempotency, and configurable scoring
"""

import os
import sys
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import time
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/scoring_job.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

class ScoringJob:
    def __init__(self):
        self.db_url = os.getenv('DB_URL', 'postgresql://postgres:postgres@postgres:5432/leadflowx')
        self.job_date = date.today()
        self.job_id = None
        self.conn = None
        
    def connect_db(self) -> bool:
        """Establish database connection with retry logic"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)
                logger.info("Database connection established")
                return True
            except Exception as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        
        return False
    
    def check_job_lock(self) -> bool:
        """Check if scoring job is already running for today"""
        try:
            with self.conn.cursor() as cur:
                # First, clean up any stale 'running' jobs older than 4 hours
                cur.execute("""
                    UPDATE scoring_jobs 
                    SET status = 'failed' 
                    WHERE job_date = %s 
                    AND status = 'running' 
                    AND start_time < NOW() - INTERVAL '4 hours'
                """, (self.job_date,))
                
                stale_jobs_updated = cur.rowcount
                if stale_jobs_updated > 0:
                    logger.warning(f"Marked {stale_jobs_updated} stale running jobs as failed")
                    self.conn.commit()
                
                # Now check for currently running jobs
                cur.execute("""
                    SELECT id, status, start_time FROM scoring_jobs 
                    WHERE job_date = %s AND status = 'running'
                    ORDER BY start_time DESC LIMIT 1
                """, (self.job_date,))
                
                result = cur.fetchone()
                if result:
                    logger.warning(f"Scoring job already running for {self.job_date} (started at {result['start_time']})")
                    return False
                
                # Check if we already completed a job today and if we should allow re-runs
                cur.execute("""
                    SELECT id, status, end_time FROM scoring_jobs 
                    WHERE job_date = %s AND status = 'completed'
                    ORDER BY end_time DESC LIMIT 1
                """, (self.job_date,))
                
                completed_result = cur.fetchone()
                if completed_result:
                    logger.info(f"Found completed job for {self.job_date}. Allowing re-run for testing purposes.")
                    # In production, you might want to return False here to prevent duplicate runs
                    # return False
                
                return True
        except Exception as e:
            logger.error(f"Error checking job lock: {e}")
            return False
    
    def create_job_record(self) -> bool:
        """Create a new scoring job record"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scoring_jobs (job_date, status, leads_processed, start_time)
                    VALUES (%s, 'running', 0, %s) RETURNING id
                """, (self.job_date, datetime.now()))
                
                self.job_id = cur.fetchone()['id']
                self.conn.commit()
                logger.info(f"Created scoring job record with ID: {self.job_id}")
                return True
        except Exception as e:
            logger.error(f"Error creating job record: {e}")
            self.conn.rollback()
            return False
    
    def get_scoring_config(self) -> Dict[str, float]:
        """Get scoring configuration from config table"""
        default_config = {
            'audit_score_weight': 0.4,
            'audit_score_threshold': 50,
            'audit_score_points': 10,
            'employee_count_min': 1,
            'employee_count_max': 250,
            'employee_count_points': 5,
            'email_exists_points': 2,
            'website_ssl_points': 3,
            'company_size_bonus': 8
        }
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT key, value FROM config WHERE key LIKE 'scoring_%'")
                config_rows = cur.fetchall()
                
                config = default_config.copy()
                for row in config_rows:
                    try:
                        config[row['key']] = float(row['value'])
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid config value for {row['key']}: {row['value']}")
                
                logger.info(f"Loaded scoring configuration: {config}")
                return config
        except Exception as e:
            logger.error(f"Error loading scoring config, using defaults: {e}")
            return default_config
    
    def calculate_lead_score(self, lead: Dict, config: Dict[str, float]) -> int:
        """Calculate lead score based on various factors"""
        score = 0
        
        # Audit score factor
        audit_score = lead.get('audit_score', 0)
        if audit_score >= config['audit_score_threshold']:
            score += config['audit_score_points']
        
        # Employee count factor (simulated for now)
        # In real implementation, this would come from enrichment data
        estimated_employees = max(1, len(lead.get('company', '')) * 10)  # Rough estimate
        if config['employee_count_min'] <= estimated_employees <= config['employee_count_max']:
            score += config['employee_count_points']
        
        # Email exists factor
        if lead.get('email'):
            score += config['email_exists_points']
        
        # Website SSL factor
        if lead.get('website', '').startswith('https://'):
            score += config['website_ssl_points']
        
        # Company size bonus (for well-known domains)
        website = lead.get('website', '').lower()
        if any(domain in website for domain in ['.com', '.org', '.net']):
            score += config['company_size_bonus']
        
        return min(score, 100)  # Cap at 100
    
    def update_lead_scores(self) -> int:
        """Update lead scores for all leads"""
        config = self.get_scoring_config()
        leads_processed = 0
        
        try:
            with self.conn.cursor() as cur:
                # Get all leads from raw_leads table
                cur.execute("""
                    SELECT id, email, company, website, correlation_id
                    FROM raw_leads 
                    ORDER BY created_at DESC
                """)
                
                leads = cur.fetchall()
                logger.info(f"Found {len(leads)} leads to score")
                
                for lead in leads:
                    try:
                        # Calculate score (audit_score defaults to 0 since we don't have audit data yet)
                        lead_dict = dict(lead)
                        lead_dict['audit_score'] = 0  # Default audit score
                        new_score = self.calculate_lead_score(lead_dict, config)
                        
                        # For now, just log the score since we don't have a lead_score column
                        # In a real implementation, you'd add this column to raw_leads or create a scores table
                        logger.info(f"Lead {lead['email']}: scored {new_score} points")
                        
                        leads_processed += 1
                        
                        if leads_processed % 100 == 0:
                            logger.info(f"Processed {leads_processed} leads...")
                    
                    except Exception as e:
                        logger.error(f"Error scoring lead {lead['id']}: {e}")
                        continue
                
                logger.info(f"Successfully processed scores for {leads_processed} leads")
                
        except Exception as e:
            logger.error(f"Error in update_lead_scores: {e}")
            self.conn.rollback()
            raise
        
        return leads_processed
    
    def cleanup_old_jobs(self):
        """Clean up old job records (keep last 30 days)"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM scoring_jobs 
                    WHERE job_date < CURRENT_DATE - INTERVAL '30 days'
                """)
                deleted = cur.rowcount
                self.conn.commit()
                logger.info(f"Cleaned up {deleted} old job records")
        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {e}")
    
    def complete_job(self, leads_processed: int, error: Optional[str] = None):
        """Mark job as completed or failed"""
        if not self.job_id:
            return
        
        try:
            with self.conn.cursor() as cur:
                status = 'failed' if error else 'completed'
                cur.execute("""
                    UPDATE scoring_jobs 
                    SET status = %s, leads_processed = %s, end_time = %s
                    WHERE id = %s
                """, (status, leads_processed, datetime.now(), self.job_id))
                self.conn.commit()
                logger.info(f"Job {self.job_id} marked as {status}")
                if error:
                    logger.error(f"Job failed with error: {error}")
        except Exception as e:
            logger.error(f"Error updating job status: {e}")
    
    def run(self) -> bool:
        """Main job execution"""
        start_time = datetime.now()
        leads_processed = 0
        
        logger.info(f"Starting scoring job for {self.job_date}")
        
        try:
            # Connect to database
            if not self.connect_db():
                return False
            
            # Check job lock
            if not self.check_job_lock():
                return False
            
            # Create job record
            if not self.create_job_record():
                return False
            
            # Process lead scoring
            leads_processed = self.update_lead_scores()
            
            # Cleanup old records
            self.cleanup_old_jobs()
            
            # Mark job as completed
            self.complete_job(leads_processed)
            
            duration = datetime.now() - start_time
            logger.info(f"Scoring job completed successfully in {duration.total_seconds():.2f}s")
            logger.info(f"Processed {leads_processed} leads")
            
            return True
            
        except Exception as e:
            error_msg = f"Scoring job failed: {e}"
            logger.error(error_msg)
            self.complete_job(leads_processed, error_msg)
            return False
            
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed")

def main():
    """Entry point for the scoring job"""
    logger.info("LeadFlowX Scoring Job starting...")
    
    job = ScoringJob()
    success = job.run()
    
    exit_code = 0 if success else 1
    logger.info(f"Scoring job finished with exit code {exit_code}")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
