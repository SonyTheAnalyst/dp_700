docs: add technical documentation for Temporal Windowing strategies

- Tumbling Windows: Implemented as fixed-sized, non-overlapping intervals.
  * Logic: A 10s window starts every 10s; events belong to exactly one bucket.
  * Example: Counting website logins in discrete 5-minute blocks.
  * Use Case: Standard periodic reporting (e.g., total sales per hour).



docs: add technical breakdown of Hopping vs. Sliding windows

. Hopping Windows (The "Predictable Overlap")
Hopping windows are scheduled. They start at fixed intervals regardless of whether data arrives or not. You define them using two fixed numbers: Window Size and Hop Size.

- The Logic: If you have a 10-minute window that hops every 5 minutes, a new window is created every 5 minutes, but it looks back at 10 minutes of data.
- The Overlap: Because the hop (5m) is smaller than the window (10m), the second half of Window A is actually the first half of Window B.
- Example: Imagine a store manager who wants a "Last 10 minutes" sales report every 5 minutes.
    * Window 1 (10:00 - 10:10): Shows sales from 10:00 to 10:10.
    * Window 2 (10:05 - 10:15): Shows sales from 10:05 to 10:15.
    * Notice: The sales that happened at 10:07 appear in both reports.



. Sliding Windows (The "Reactionary Overlap")
Sliding windows are more "fluid." In systems like Spark, a sliding window is often used to evaluate a condition over a period of time relative to each event.

- The Logic: They are not fixed to a clock (like 10:05, 10:10). Instead, the window "slides" as events move through time. It is typically used to see if a certain threshold is met within any interval of a specific length.
- The Difference: While a Hopping window says "Give me a report every 5 minutes," a Sliding window says "Every time a new event arrives, look back at the last 10 minutes."
- Example: A security alarm.
    * If a sensor is tripped at 10:01, 10:02, and 10:03, a sliding window of 5 minutes checks: "Have there been 3 trips in the last 5 minutes?"
    * The window doesn't wait for 10:05 to check; it evaluates as the events happen.

- Session Windows: Implemented as activity-based groups that filter periods of inactivity.
  * Logic: Dynamic duration governed by a 'gap timeout' (session closes after X minutes of silence).
  * Example: Grouping clickstream data until a user is inactive for 5 minutes.
  * Use Case: User behavior analysis and sessionized clickstream processing.
