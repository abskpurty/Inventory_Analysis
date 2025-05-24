# Inventory-Analysis

This SQL-based project focuses on the analysis of inventory operations across multiple stores, aiming to uncover inefficiencies, quantify stockout-related losses, assess vendor reliability, and recommend optimal inventory management practices. The core objective is to help businesses reduce lost revenue from stockouts, optimize reorder behavior, and determine safety stock levels based on demand and lead time variability. The data was simulated using Python.

Objectives:

1. Quantifying the profit lost due to stockouts.
2. Classifying products into A/B/C segments based on their profit contribution and out-of-stock impact.
3. Recommending safety stock levels based on statistical analysis of demand and lead time.
4. Analyzing vendor-level reliability.
5. Assessing whether orders were placed timely with respect to the reorder point.

Concepts Used:

WINDOW FUNCTIONS, JOINs, CTEs, CASE, DATEDIFF, IFNULL, POW, STDDEV.

Summary of Insights:
1. Profit Impact: The company lost about 15% of profit due to stockouts. 65% of this lost profit was associated with stockouts that occurred after inventory fell below the reorder point. 
2. ABC Classification: 39% of items contribute 80% of profit and deserve the highest service level.
3. Reorder Behavior: Majority of the orders in all stores were made below reorder point. Highest being 89% of all orders for a particular store.  
4. Vendor-Level Insights: Certain vendors were highly inconsistent when it comes to delivery times. These vendors require attention or renegotiation.
