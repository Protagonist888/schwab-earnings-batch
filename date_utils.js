/**
 * ============================================================================
 * UNIFIED DATE UTILITIES MODULE
 * ============================================================================
 * 
 * Provides timezone-safe date handling for earnings calculations across:
 * - process-earnings.js (Node.js batch script)
 * - background.js (Chrome extension service worker)
 * - content_script_unified.js (Chrome extension content script)
 * 
 * @version 1.0.0
 * @author AlphaNudge Team
 * 
 * CRITICAL DESIGN DECISIONS:
 * - All dates normalized to local midnight (00:00:00)
 * - API date strings ("YYYY-MM-DD") parsed in local timezone
 * - Consistent comparison logic prevents off-by-one errors
 * - Handles Daylight Saving Time transitions
 * 
 * ============================================================================
 */

class DateUtils {
  /**
   * Get today's date normalized to local midnight
   * Ensures consistent "today" reference across all calculations
   * 
   * @returns {Date} Date object set to 00:00:00.000 in local timezone
   * 
   * @example
   * const today = DateUtils.getTodayNormalized();
   * // Returns: 2025-11-08T00:00:00.000 (local timezone)
   */
  static getTodayNormalized() {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    return today;
  }

  /**
   * Parse API date string and normalize to local midnight
   * 
   * CRITICAL: API returns dates as "YYYY-MM-DD" strings
   * Using `new Date("YYYY-MM-DD")` interprets as UTC midnight
   * This causes timezone issues for users not in UTC
   * 
   * Solution: Parse components and create Date in local timezone
   * 
   * @param {string} dateString - Format: "YYYY-MM-DD" (e.g., "2025-03-15")
   * @returns {Date|null} Date object at 00:00:00 local time, or null if invalid
   * 
   * @example
   * // User in PST (UTC-8) at 5pm local time
   * DateUtils.parseApiDate("2025-03-15");
   * // Returns: 2025-03-15T00:00:00.000 PST (correct!)
   * // NOT: 2025-03-14T16:00:00.000 PST (wrong - UTC interpretation)
   */
  static parseApiDate(dateString) {
    if (!dateString || typeof dateString !== 'string') {
      return null;
    }
    
    // Validate format: YYYY-MM-DD
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(dateString)) {
      return null;
    }
    
    // Parse components manually to force local timezone
    const [year, month, day] = dateString.split('-').map(Number);
    
    // Validate components
    if (year < 1900 || year > 2100) return null;
    if (month < 1 || month > 12) return null;
    if (day < 1 || day > 31) return null;
    
    // Create Date in local timezone (month is 0-indexed)
    const date = new Date(year, month - 1, day, 0, 0, 0, 0);
    
    // Validate the date was created correctly (handles invalid dates like Feb 31)
    if (date.getFullYear() !== year || 
        date.getMonth() !== month - 1 || 
        date.getDate() !== day) {
      return null;
    }
    
    return date;
  }

  /**
   * Calculate days until target date from today
   * 
   * Returns positive for future dates, negative for past dates
   * Uses ceiling to always round up partial days
   * 
   * @param {string} dateString - Target date "YYYY-MM-DD"
   * @returns {number|null} Days until (negative if past), or null if invalid
   * 
   * @example
   * // Today is 2025-11-08
   * DateUtils.daysUntil("2025-11-18"); // Returns: 10
   * DateUtils.daysUntil("2025-11-01"); // Returns: -7
   * DateUtils.daysUntil("2025-11-08"); // Returns: 0 (today)
   */
  static daysUntil(dateString) {
    const today = this.getTodayNormalized();
    const targetDate = this.parseApiDate(dateString);
    
    if (!targetDate) {
      return null;
    }
    
    const diffTime = targetDate - today;
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    return diffDays;
  }

  /**
   * Check if date is in the past (before today)
   * 
   * @param {string} dateString - Date to check "YYYY-MM-DD"
   * @returns {boolean} True if date is before today, false otherwise
   * 
   * @example
   * // Today is 2025-11-08
   * DateUtils.isPastDate("2025-11-01"); // Returns: true
   * DateUtils.isPastDate("2025-11-08"); // Returns: false (today is not past)
   * DateUtils.isPastDate("2025-11-15"); // Returns: false
   */
  static isPastDate(dateString) {
    const daysUntil = this.daysUntil(dateString);
    return daysUntil !== null && daysUntil < 0;
  }

  /**
   * Check if date is in the future (after today)
   * 
   * @param {string} dateString - Date to check "YYYY-MM-DD"
   * @returns {boolean} True if date is after today, false otherwise
   * 
   * @example
   * // Today is 2025-11-08
   * DateUtils.isFutureDate("2025-11-01"); // Returns: false
   * DateUtils.isFutureDate("2025-11-08"); // Returns: false (today is not future)
   * DateUtils.isFutureDate("2025-11-15"); // Returns: true
   */
  static isFutureDate(dateString) {
    const daysUntil = this.daysUntil(dateString);
    return daysUntil !== null && daysUntil > 0;
  }

  /**
   * Check if date is today
   * 
   * @param {string} dateString - Date to check "YYYY-MM-DD"
   * @returns {boolean} True if date is today
   * 
   * @example
   * // Today is 2025-11-08
   * DateUtils.isToday("2025-11-08"); // Returns: true
   * DateUtils.isToday("2025-11-09"); // Returns: false
   */
  static isToday(dateString) {
    const daysUntil = this.daysUntil(dateString);
    return daysUntil === 0;
  }

  /**
   * Format date for display in user-friendly format
   * 
   * @param {string} dateString - Date "YYYY-MM-DD"
   * @returns {string} Formatted date (e.g., "Mar 15, 2025")
   * 
   * @example
   * DateUtils.formatDisplayDate("2025-03-15"); // Returns: "Mar 15, 2025"
   * DateUtils.formatDisplayDate("invalid"); // Returns: "Unknown"
   */
  static formatDisplayDate(dateString) {
    const date = this.parseApiDate(dateString);
    if (!date) {
      return 'Unknown';
    }
    
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });
  }

  /**
   * Get date N days in the future or past from today
   * 
   * @param {number} days - Number of days (positive = future, negative = past)
   * @returns {Date} Date object normalized to midnight
   * 
   * @example
   * // Today is 2025-11-08
   * DateUtils.getDateOffset(10);  // Returns: 2025-11-18 00:00:00
   * DateUtils.getDateOffset(-7);  // Returns: 2025-11-01 00:00:00
   * DateUtils.getDateOffset(0);   // Returns: 2025-11-08 00:00:00 (today)
   */
  static getDateOffset(days) {
    const date = this.getTodayNormalized();
    date.setDate(date.getDate() + days);
    return date;
  }

  /**
   * Format Date object as API string "YYYY-MM-DD"
   * 
   * @param {Date} date - Date object
   * @returns {string|null} Formatted string "YYYY-MM-DD", or null if invalid
   * 
   * @example
   * const date = new Date(2025, 2, 15); // March 15, 2025
   * DateUtils.formatApiDate(date); // Returns: "2025-03-15"
   */
  static formatApiDate(date) {
    if (!(date instanceof Date) || isNaN(date.getTime())) {
      return null;
    }
    
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    
    return `${year}-${month}-${day}`;
  }

  /**
   * Find next future date from array of date strings
   * Returns the earliest date that is after today
   * 
   * @param {string[]} dates - Array of "YYYY-MM-DD" strings
   * @returns {string|null} Next future date or null if none found
   * 
   * @example
   * // Today is 2025-11-08
   * const dates = ["2025-11-01", "2025-11-15", "2025-11-10", "2025-12-01"];
   * DateUtils.findNextFutureDate(dates); // Returns: "2025-11-10"
   */
  static findNextFutureDate(dates) {
    if (!Array.isArray(dates) || dates.length === 0) {
      return null;
    }
    
    const today = this.getTodayNormalized();
    
    const futureDates = dates
      .filter(dateStr => {
        const date = this.parseApiDate(dateStr);
        return date && date > today;
      })
      .sort((a, b) => {
        const dateA = this.parseApiDate(a);
        const dateB = this.parseApiDate(b);
        return dateA - dateB;
      });
    
    return futureDates.length > 0 ? futureDates[0] : null;
  }

  /**
   * Find most recent past date from array of date strings
   * Returns the latest date that is before or equal to today
   * 
   * @param {string[]} dates - Array of "YYYY-MM-DD" strings
   * @returns {string|null} Most recent past date or null if none found
   * 
   * @example
   * // Today is 2025-11-08
   * const dates = ["2025-11-01", "2025-11-05", "2025-11-15"];
   * DateUtils.findMostRecentPastDate(dates); // Returns: "2025-11-05"
   */
  static findMostRecentPastDate(dates) {
    if (!Array.isArray(dates) || dates.length === 0) {
      return null;
    }
    
    const today = this.getTodayNormalized();
    
    const pastDates = dates
      .filter(dateStr => {
        const date = this.parseApiDate(dateStr);
        return date && date <= today;
      })
      .sort((a, b) => {
        const dateA = this.parseApiDate(a);
        const dateB = this.parseApiDate(b);
        return dateB - dateA; // Descending order (most recent first)
      });
    
    return pastDates.length > 0 ? pastDates[0] : null;
  }

  /**
   * Get date range for queries
   * Useful for constructing API URLs
   * 
   * @param {number} daysBack - Days in the past
   * @param {number} daysForward - Days in the future
   * @returns {{from: string, to: string}} Object with 'from' and 'to' dates
   * 
   * @example
   * // Today is 2025-11-08
   * DateUtils.getDateRange(30, 90);
   * // Returns: { from: "2025-10-09", to: "2026-02-06" }
   */
  static getDateRange(daysBack, daysForward) {
    const fromDate = this.getDateOffset(-daysBack);
    const toDate = this.getDateOffset(daysForward);
    
    return {
      from: this.formatApiDate(fromDate),
      to: this.formatApiDate(toDate)
    };
  }

  /**
   * Check if date string is valid format
   * 
   * @param {string} dateString - Date to validate
   * @returns {boolean} True if valid "YYYY-MM-DD" format
   * 
   * @example
   * DateUtils.isValidDateFormat("2025-03-15"); // Returns: true
   * DateUtils.isValidDateFormat("2025-3-15");  // Returns: false (no leading zero)
   * DateUtils.isValidDateFormat("03/15/2025"); // Returns: false (wrong format)
   */
  static isValidDateFormat(dateString) {
    if (!dateString || typeof dateString !== 'string') {
      return false;
    }
    
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(dateString)) {
      return false;
    }
    
    const date = this.parseApiDate(dateString);
    return date !== null;
  }

  /**
   * Get business days until date (excludes weekends)
   * Approximation - does not account for holidays
   * 
   * @param {string} dateString - Target date "YYYY-MM-DD"
   * @returns {number|null} Business days until, or null if invalid
   * 
   * @example
   * // Today is 2025-11-08 (Saturday)
   * DateUtils.getBusinessDaysUntil("2025-11-18"); // Returns: ~7 (excludes 2 weekends)
   */
  static getBusinessDaysUntil(dateString) {
    const today = this.getTodayNormalized();
    const targetDate = this.parseApiDate(dateString);
    
    if (!targetDate) {
      return null;
    }
    
    let businessDays = 0;
    const currentDate = new Date(today);
    
    while (currentDate < targetDate) {
      const dayOfWeek = currentDate.getDay();
      if (dayOfWeek !== 0 && dayOfWeek !== 6) { // Not Sunday (0) or Saturday (6)
        businessDays++;
      }
      currentDate.setDate(currentDate.getDate() + 1);
    }
    
    return businessDays;
  }

  /**
   * Find price date with lookback for weekends/holidays
   * Searches backwards up to N days to find a valid trading day
   * 
   * @param {Array} priceData - Array of {date: "YYYY-MM-DD", close: number} objects
   * @param {string} targetDateString - Target date "YYYY-MM-DD"
   * @param {number} maxLookbackDays - Maximum days to search backwards (default: 5)
   * @returns {number|null} Close price, or null if not found
   * 
   * @example
   * const prices = [
   *   {date: "2025-11-07", close: 150.25},
   *   {date: "2025-11-06", close: 149.80}
   * ];
   * // Looking for 2025-11-09 (Sunday) - will find 2025-11-07 (Friday)
   * DateUtils.findPriceOnDate(prices, "2025-11-09"); // Returns: 150.25
   */
  static findPriceOnDate(priceData, targetDateString, maxLookbackDays = 5) {
    if (!Array.isArray(priceData) || priceData.length === 0) {
      return null;
    }
    
    const targetDate = this.parseApiDate(targetDateString);
    if (!targetDate) {
      return null;
    }
    
    // Try exact match first
    const exactMatch = priceData.find(d => d.date === targetDateString);
    if (exactMatch && exactMatch.close !== undefined) {
      return parseFloat(exactMatch.close);
    }
    
    // Search backwards for up to maxLookbackDays
    for (let i = 1; i <= maxLookbackDays; i++) {
      const lookbackDate = new Date(targetDate);
      lookbackDate.setDate(lookbackDate.getDate() - i);
      const lookbackDateString = this.formatApiDate(lookbackDate);
      
      const match = priceData.find(d => d.date === lookbackDateString);
      if (match && match.close !== undefined) {
        return parseFloat(match.close);
      }
    }
    
    return null;
  }

  /**
   * Calculate percentage change between two dates
   * Used for earnings move calculations
   * 
   * @param {Array} priceData - Array of {date: "YYYY-MM-DD", close: number}
   * @param {string} beforeDate - Date before event "YYYY-MM-DD"
   * @param {string} afterDate - Date after event "YYYY-MM-DD"
   * @returns {number|null} Percentage change, or null if data unavailable
   * 
   * @example
   * const prices = [{date: "2025-11-07", close: 100}, {date: "2025-11-08", close: 105}];
   * DateUtils.calculatePercentChange(prices, "2025-11-07", "2025-11-08");
   * // Returns: 5.0 (5% increase)
   */
  static calculatePercentChange(priceData, beforeDate, afterDate) {
    const beforePrice = this.findPriceOnDate(priceData, beforeDate);
    const afterPrice = this.findPriceOnDate(priceData, afterDate);
    
    if (!beforePrice || !afterPrice || beforePrice === 0) {
      return null;
    }
    
    return ((afterPrice - beforePrice) / beforePrice) * 100;
  }
}

// =============================================================================
// EXPORT FOR DIFFERENT ENVIRONMENTS
// =============================================================================

// Export for Node.js (process-earnings.js)
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { DateUtils };
}

// Export for Service Worker (background.js)
if (typeof self !== 'undefined' && typeof self.DateUtils === 'undefined') {
  self.DateUtils = DateUtils;
}

// Export for content script (if loaded as module)
if (typeof window !== 'undefined' && typeof window.DateUtils === 'undefined') {
  window.DateUtils = DateUtils;
}