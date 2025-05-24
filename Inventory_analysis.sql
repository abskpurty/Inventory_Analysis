-- Use the 'inventory' database
USE inventory_analysis;

-- Create a table to store inventory levels at the beginning of the period
CREATE TABLE inventory_begin(
    InventoryID VARCHAR(15),
    Item VARCHAR(15),
    Store INT,
    Brand INT,
    Onhand INT,
    Price DOUBLE,
    StartDate DATE
);

-- Load beginning inventory data
LOAD DATA INFILE 'inventory_begin.csv'
INTO TABLE inventory_begin
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Create a table to store inventory levels at the end of the period
CREATE TABLE inventory_end(
    InventoryID VARCHAR(15),
    Item VARCHAR(15),
    Store INT,
    Brand INT,
    Onhand INT,
    Price DOUBLE,
    EndDate DATE
);
-- Load beginning inventory data
LOAD DATA INFILE 'inventory_end.csv'
INTO TABLE inventory_end
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Create a table to store detailed purchase records
CREATE TABLE purchases(
    InventoryID VARCHAR(15),
    Item VARCHAR(15),
    Store INT,
    Brand INT,
    VendorNumber INT,
    VendorName VARCHAR(40),
    PONumber INT,
    PODate DATE,
    ReceivingDate DATE,
    InvoiceDate DATE,
    PayDate DATE,
    PurchasePrice DOUBLE,
    Quantity INT,
    PayAmount DOUBLE
);

-- Load purchase transaction data
LOAD DATA INFILE 'purchases.csv'
INTO TABLE purchases 
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- Create a table to store sales transactions
CREATE TABLE sales(
    InventoryID VARCHAR(15),
    Item VARCHAR(15),
    Store INT,
    Brand INT,
    SalesQuantity INT,
    SalesPrice DOUBLE,
    SalesTotal DOUBLE,
    SalesDate DATE
);

-- Load sales data
LOAD DATA INFILE 'sales.csv'
INTO TABLE sales 
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- The following table lists all the unique dates on which inventory was updated, along with the stock level on that day.
CREATE TEMPORARY TABLE InventoryUpdates
WITH Updates AS (
	-- Combine initial inventory
    SELECT InventoryID, Onhand AS Quantity, StartDate AS Date 
     FROM inventory_begin
    UNION
    -- Add purchases
    SELECT InventoryID, Quantity, ReceivingDate AS Date 
     FROM purchases
    UNION
    -- Subtract sales
    SELECT InventoryID, -1 * SalesQuantity AS Quantity, SalesDate AS Date 
     FROM Sales
),
AggUpdates AS (
	SELECT InventoryID, SUM(Quantity) AS Quantity, date
    FROM Updates
    GROUP BY InventoryID, Date
)
SELECT *,
SUM(Quantity) OVER (
	PARTITION BY InventoryID 
	ORDER BY Date
	ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS CurrentStock
FROM AggUpdates;

-- The following table lists all the orders that arrived late.  
-- Late arrivals will be identified by the ordering of days on which inventory was updated, either by sales or purchase. 
-- If an update date is a day of stockout and is followed by an update date on which an order arrived, then that order is a late arrival. 

CREATE TEMPORARY TABLE LateArrival 
WITH TypeOfDay AS (
	SELECT InventoryID, PODate, DayType, Date,
	LAG(DayType) OVER (PARTITION BY InventoryID ORDER BY Date, Pref, PODate) AS PrevDayType,
    LAG(Date) OVER (PARTITION BY InventoryID ORDER BY Date, Pref, PODate) AS PrevDate
	FROM 
	(SELECT InventoryID, 3 AS Pref, NULL AS PODate, Date, 'StockOut_Day' AS DayType FROM InventoryUpdates WHERE CurrentStock=0
	UNION 
	SELECT InventoryID, 2 AS Pref, NULL AS PODate, PODate AS Date, 'Purchase_Day' AS DayType FROM purchases
    UNION
    SELECT InventoryID, 1 AS Pref, PODate, ReceivingDate AS Date, 'Receiving_Day' AS DayType FROM purchases) AS T
)
SELECT *
FROM TypeOfDay
WHERE DayType='Receiving_Day' AND PrevDayType='StockOut_Day'; 


-- The table stores the date of purchase order and the latest day of inventory update.
CREATE TEMPORARY TABLE DateBeforePO 
WITH UpdateDateAndPODate AS (
	SELECT ip.InventoryID, Date, PODate
	FROM InventoryUpdates AS ip
	INNER JOIN purchases AS p
	ON ip.InventoryID = p.InventoryID AND Date <= PODate
)
SELECT InventoryID, PODate, MAX(Date) AS RecentUpdateDate
FROM UpdateDateAndPODate
GROUP BY InventoryID, PODate;

-- The table lists the respective inventory levels on the dates of the purchase orders.
CREATE TEMPORARY TABLE InventoryOnPODate 
SELECT T1.InventoryID, T1.Date, PODate, CurrentStock
FROM InventoryUpdates AS T1
INNER JOIN DateBeforePO AS T2
ON T1.InventoryID = T2.InventoryID AND T1.Date=T2.RecentUpdateDate;

DROP TABLE DateBeforePO; 

SET @StartDate = (SELECT MIN(StartDate) FROM inventory_begin);
SET @EndDate = (SELECT MAX(EndDate) FROM inventory_end);
SET @TotalNumDays = DATEDIFF(@EndDate, @StartDate)+ 1;
SELECT @TotalNumDays;

select * from inventoryupdates where InventoryID = 'ITEM_0009_10';

select * from purchases where InventoryID = 'ITEM_0009_10';
-- 'ITEM_0009_10', 'ITEM_0009', '10', '6', '1', 'VENDOR_001', '4758', '2025-04-21', '2025-05-01', '2025-04-23', '2025-05-06', '94', '92', '8674'
 
-- The table contains how long each stockout lasted.
CREATE TEMPORARY TABLE DaysWithNoStock
WITH OnlyStockOuts AS(
	SELECT S.InventoryID, Date, 
		IFNULL(MIN(ReceivingDate), @EndDate) AS NextReceivingDate, 
		DATEDIFF(IFNULL(MIN(ReceivingDate), @EndDate), Date) AS DaysNoStock 
	FROM (
		SELECT InventoryID, Date
		FROM InventoryUpdates 
		WHERE CurrentStock=0
	) AS S
	INNER JOIN (
		SELECT InventoryID, ReceivingDate
		FROM purchases
	) AS P
	ON S.InventoryID = P.InventoryID AND Date <= ReceivingDate
	GROUP BY S.InventoryID, Date
	ORDER BY InventoryID
),
UniqueIDs AS (
	SELECT DISTINCT InventoryID
    FROM ( SELECT InventoryID FROM inventory_begin UNION SELECT InventoryID FROM purchases) AS T
)
SELECT InventoryID, Date, NextReceivingDate, IFNULL(DaysNoStock, 0) AS DaysNoStock
FROM OnlyStockOuts
RIGHT JOIN UniqueIDs
USING(InventoryID)
ORDER BY DaysNoStock DESC, InventoryID;

-- The table contains info on profit of each inventoryid. The storage costs are taken into account in the calculation. 
CREATE TEMPORARY TABLE ProfitTable
WITH SalesByInventory AS (
	-- Calculate total revenue per InventoryID
	SELECT InventoryID, SUM(SalesQuantity) AS TotalSalesQuantity, SUM(SalesTotal) AS Revenue
    FROM sales
    GROUP BY InventoryID
),
AvgCostByInventory AS (
	SELECT InventoryID, SUM(PurchasePrice), SUM(Quantity) AS TotalPurchaseQuantity, AVG(PurchasePrice) AS AvgCost
    FROM (SELECT InventoryID, Quantity, PurchasePrice FROM purchases
		  UNION 
          SELECT InventoryID, OnHand AS Quantity, Price AS PurchasePrice FROM inventory_begin WHERE Onhand!=0) AS T
    GROUP BY InventoryID
),
DaysWithoutStock AS(
	SELECT InventoryID, SUM(DaysNoStock) AS SumDaysNoStock 
    FROM DaysWithNoStock 
    GROUP BY InventoryID
),
ProfitByInventory AS (
	SELECT InventoryID, (Revenue - AvgCost*TotalSalesQuantity) AS Profit
    FROM SalesByInventory
    INNER JOIN AvgCostByInventory
    USING(InventoryID)
),
AvgProfitByInventory AS (
	SELECT InventoryID,
		Profit, SumDaysNoStock, 
        IF(@TotalNumDays-SumDaysNoStock!=0,Profit/(@TotalNumDays-SumDaysNoStock), 0) AS AvgProfit
    FROM ProfitByInventory
    INNER JOIN DaysWithoutStock
    USING(InventoryID)
)
SELECT InventoryID, Profit, AvgProfit, SumDaysNoStock,
Profit+AvgProfit*SumDaysNoStock AS AdjustedProfit, 
AvgProfit*SumDaysNoStock AS OutOfStockLoss
FROM AvgProfitByInventory;

SET @TotalProfit = (SELECT SUM(Profit) FROM ProfitTable);
SELECT @TotalProfit;  -- 11.3 M

-- Total profit made by all the stores.
SET @TotalAdjustedProfit = (SELECT SUM(AdjustedProfit) FROM ProfitTable);
SELECT @TotalAdjustedProfit; -- 13 M

-- Profit lost due to items being out-of-stock ~ 15%

-- Divides the inventories into class A, B and C, on the basis of the profits they would have made if there were no stockouts in addition to the profits that they actually made. 
CREATE TEMPORARY TABLE ABC 
WITH CumulativeRevenue AS (
	-- Calculate cumulative revenue share
	SELECT *,
	SUM(AdjustedProfit) OVER (ORDER BY AdjustedProfit DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / (@TotalAdjustedProfit) AS CumPerc
	FROM ProfitTable
)
-- Assign ABC label based on cumulative revenue
SELECT *,
CASE WHEN CumPerc<=0.7 THEN 'A' 
     WHEN CumPerc>0.7 AND CumPerc<=0.9 THEN 'B'
     WHEN CumPerc>0.9 THEN 'C' 
     END AS Label
     FROM CumulativeRevenue
     ORDER BY SumDaysNoStock DESC;
     
SELECT InventoryID, Label, OutOfStockLoss, NumOfStockOuts
FROM (
	SELECT InventoryID, COUNT(*) AS NumOfStockOuts
	FROM InventoryUpdates
	WHERE CurrentStock=0
	GROUP BY InventoryID
) AS T1
LEFT JOIN (
	SELECT DISTINCT InventoryID, Label, OutOfStockLoss
	FROM ABC
) AS T2
USING(InventoryID)
ORDER BY NumOfStockOuts DESC, OutOfStockLoss DESC;
-- ProfitTable has no use left. 
DROP TABLE ProfitTable;

-- ------------------------
-- Observations
-- ------------------------

-- Proportion of inventories labeled A = 38.8%
-- Proportion of inventories labeled B = 28.5%
-- Proportion of inventories labeled B = 32.7%

-- About 39% percent of inventories can potentially contribute to 80% of profit if inventory is properly managed. 


-- ------------------------------------
-- Safety Stock Calculation
-- ------------------------------------
-- The table contains safety stock, the associated service levels are adjusted as per label(A,B,C).    
CREATE TEMPORARY TABLE SafetyStockTable
WITH AverageLeadTime AS (
	SELECT InventoryID, AVG(DATEDIFF(ReceivingDate, PODate)) AS AvgLeadTime
	FROM purchases
	GROUP BY InventoryID
),
LeadTimeVariance AS (
	SELECT InventoryID, AvgLeadTime, 
		SUM(POW(AvgLeadTime - DATEDIFF(ReceivingDate, PODate), 2)) / COUNT(*) AS LTVariance
	FROM purchases
	INNER JOIN AverageLeadTime
	USING(InventoryID)
	GROUP BY InventoryID
),
AverageDemand AS (
	SELECT InventoryID, SumDaysNoStock, IF(@TotalNumDays - SumDaysNoStock!=0,SumDemand/(@TotalNumDays - SumDaysNoStock),0) AS AvgDemand
    FROM (SELECT InventoryID, SUM(SalesQuantity) AS SumDemand FROM sales GROUP BY InventoryID) AS T1
    INNER JOIN (SELECT InventoryID, SUM(DaysNoStock) AS SumDaysNoStock FROM DaysWithNoStock GROUP BY InventoryID) AS T2
    USING(InventoryID)
),
DemandVariance AS (
	SELECT InventoryID, AvgDemand, 
		IF(@TotalNumDays-SumDaysNoStock!=0, (SUM(POW(AvgDemand-SalesQuantity, 2)) + POW(AvgDemand,2) * (@TotalNumDays-COUNT(*) - SumDaysNoStock)) / 
        (@TotalNumDays-SumDaysNoStock), 0) AS DVariance
	FROM sales
	INNER JOIN AverageDemand
	USING(InventoryID)
	GROUP BY InventoryID
),
SS_intermediate AS (
	SELECT InventoryID, Label, AvgLeadTime, AvgDemand, LTVariance, DVariance, 
		SQRT(AvgLeadTime * DVariance + POW(AvgDemand, 2) * LTVariance) AS SS_
	FROM LeadTimeVariance
	INNER JOIN DemandVariance 
    USING(InventoryID)
	INNER JOIN (
		SELECT DISTINCT InventoryID, Label FROM ABC
	) AS LabelTable 
    USING(InventoryID)
)
-- The following formula assumes 
SELECT InventoryID, ROUND(AvgLeadTime,2) AS AvgLeadTime, ROUND(AvgDemand,2) AS AvgDemand, ROUND(LTVariance,2) AS LTVariance, ROUND(DVariance,2) AS DVariance, Label, 
	CASE 
		WHEN Label = 'A' THEN ROUND(1.65 * SS_, 0) -- ~95% Service level
		WHEN Label = 'B' THEN ROUND(1.28 * SS_, 0) -- ~90% Service level
		WHEN Label = 'C' THEN ROUND(1.04 * SS_, 0) -- ~85% Service level
	END AS SafetyStock
FROM SS_intermediate;
 

-- Following query merges previous tables to get information on inventory level when order was placed, i.e, whether the inventory was 
-- above reorder point or below reorder point when the order was placed 

CREATE TEMPORARY TABLE StockOutDetails
SELECT InventoryID, Label, PODate, PrevDate AS StockOutDate, 
	la.Date AS ReceivingDate_AfterStockOut, CurrentStock, 
	ReOrderPoint, NumOfStockOuts, OutOfStockLoss,
	Store, Brand, VendorName, VendorNumber 
FROM InventoryOnPODate
LEFT JOIN LateArrival AS la
USING(InventoryID,PODate)  
LEFT JOIN (SELECT InventoryID, (AvgDemand * AvgLeadTime + SafetyStock) AS ReorderPoint FROM SafetyStockTable) AS ROPTable
USING(InventoryID)
LEFT JOIN (SELECT InventoryID, COUNT(*) AS NumOfStockOuts FROM InventoryUpdates WHERE CurrentStock=0 GROUP BY InventoryID) AS NumberStockOuts
USING(InventoryID)
LEFT JOIN (SELECT DISTINCT InventoryID, Store, Brand, VendorName, VendorNumber FROM purchases) AS Item_details
USING(InventoryID)
LEFT JOIN ABC
USING(InventoryID);


-- Following query analyzes inventory performance by label, combining reorder behavior and out-of-stock financial losses due to late arrivals. 
WITH ReorderDetails AS (
SELECT
    Label,
    SUM(IF(CurrentStock >= ReOrderPoint, 1, 0)) AS AtOrAboveReorder,
    SUM(IF(CurrentStock >= ReOrderPoint AND StockOutDate IS NOT NULL, 1, 0)) AS AtOrAboveReorderWithStockout,
    SUM(IF(CurrentStock >= ReOrderPoint AND StockOutDate IS NULL, 1, 0)) AS AtOrAboveReorderNoStockout,
    SUM(IF(CurrentStock < ReOrderPoint, 1, 0)) AS BelowReorder,
    SUM(IF(CurrentStock < ReOrderPoint AND StockOutDate IS NOT NULL, 1, 0)) AS BelowReorderWithStockout,
    SUM(IF(CurrentStock < ReOrderPoint AND StockOutDate IS NULL AND CurrentStock = 0, 1, 0)) AS OrderAfterStockout,
    SUM(IF(CurrentStock < ReOrderPoint AND StockOutDate IS NULL AND CurrentStock != 0, 1, 0)) AS BelowReorderNoStockout
FROM
    StockOutDetails
WHERE 
	Label IS NOT NULL
GROUP BY
    Label
),
LateArrivalNoStock AS (
	SELECT l.InventoryID, SUM(DaysNoStock) AS TotalDaysNoStock
	FROM LateArrival AS l
	INNER JOIN DaysWithNoStock AS d
	ON l.InventoryID=d.InventoryID AND l.PrevDate=d.Date AND l.Date=d.NextReceivingDate
    GROUP BY l.InventoryID
),
LateArrivalOOSL AS (
	SELECT 
		Label, 
        ROUND(SUM(Profit),0) AS Total_Profit,
        ROUND(SUM(IFNULL(AvgProfit*TotalDaysNoStock, 0)),0) AS LateArrival_OOSL,
        ROUND(SUM(IFNULL(AvgProfit*TotalDaysNoStock, 0))*100/SUM(Profit),2) AS LateArrival_OOSL_TotalProfit_percent,
        ROUND(SUM(OutOfStockLoss),0) AS Total_OOSL,
        ROUND(SUM(OutOfStockLoss)*100/SUM(Profit),2) AS Total_OOSL_TotalProfit_percent
    FROM LateArrivalNoStock
    RIGHT JOIN ABC 
    USING(InventoryID)
    GROUP BY Label
)
SELECT *
FROM LateArrivalOOSL
INNER JOIN ReorderDetails
USING(Label);

-- ---------------
-- Observations
-- ---------------

-- All three labels have more or less the same percentage of their respective profit lost due to stockout.
-- Label A has higher fraction of orders placed below reorder point (85%), compared to B (80%) and C (66%).
-- Labels A and B are very similar, with roughly 32% of orders placed below reorder point (ROP) resulting in a stockout. Label C has a noticeably higher proportion at 36.2%
-- All three labels show a relatively low fraction of orders that were placed after stockout(around 6-7%).
-- Labels A and B show a high proportion (around 61%) of orders placed below ROP not leading to a stockout. 
-- Label C has a slightly lower proportion at 57.3%. This complements the higher stockout rate for Label C when below ROP
-- This could be due to unexpected drops in demand, faster-than-expected replenishment, or the safety stock still being just enough despite the trigger being missed.
-- Label A stands out with very low proportion of stockouts when an order was placed at or above the reorder point (only 0.93%)
-- Labels B and C have higher, but still relatively low, proportions of stockouts at 2.58% and 2.91% respectively.

-- ------------
-- Conclusion
-- ------------

-- The reorder point takes into account the expected demand during lead time and also incorporates the lead time and demand variability.
-- It's designed to ensure that new stock arrives before you run out, even if demand is a bit higher or the delivery is a bit slower than average.
-- Thus it is very likely that stockouts associated with below ROP orders could have been avoided if they were placed earlier, i.e, above ROP.   
-- The low incidence of stockouts when orders are placed above the ROP provides strong evidence that the ROP mechanism is highly effective at mitigating stockout risk.  


-- Following query analyzes stockout losses as well as ordering behaviour of stores.
WITH ReorderDetails AS (
SELECT
    Store,
    SUM(IF(CurrentStock >= ReOrderPoint, 1, 0)) AS AtOrAboveReorder,
    SUM(IF(CurrentStock >= ReOrderPoint AND StockOutDate IS NOT NULL, 1, 0)) AS AtOrAboveReorderWithStockout,
    SUM(IF(CurrentStock >= ReOrderPoint AND StockOutDate IS NULL, 1, 0)) AS AtOrAboveReorderNoStockout,
    SUM(IF(CurrentStock < ReOrderPoint, 1, 0)) AS BelowReorder,
    SUM(IF(CurrentStock < ReOrderPoint AND StockOutDate IS NOT NULL, 1, 0)) AS BelowReorderWithStockout,
    SUM(IF(CurrentStock < ReOrderPoint AND StockOutDate IS NULL AND CurrentStock = 0, 1, 0)) AS OrderAfterStockout,
    SUM(IF(CurrentStock < ReOrderPoint AND StockOutDate IS NULL AND CurrentStock != 0, 1, 0)) AS BelowReorderNoStockout
FROM
    StockOutDetails
WHERE 
	Label IS NOT NULL
GROUP BY
    Store
),
LateArrivalNoStock AS (
	SELECT l.InventoryID, SUM(DaysNoStock) AS TotalDaysNoStock 
	FROM LateArrival AS l
	INNER JOIN DaysWithNoStock AS d
	ON l.InventoryID=d.InventoryID AND l.PrevDate=d.Date AND l.Date=d.NextReceivingDate
    GROUP BY InventoryID
),
LateArrivalOOSL AS (
	SELECT 
		Store,
        ROUND(SUM(Profit),2) AS Profit,
        ROUND(SUM(IFNULL(AvgProfit*TotalDaysNoStock, 0)),2) AS LateArrival_OOSL,
        ROUND((SUM(IFNULL(AvgProfit*TotalDaysNoStock, 0))*100)/SUM(Profit),2) AS LateArrival_OOSL_Profit_Perc
    FROM LateArrivalNoStock AS l
    RIGHT JOIN ABC 
    USING(InventoryID)
    INNER JOIN
		(SELECT InventoryID, Store FROM inventory_begin UNION SELECT InventoryID, Store FROM purchases) AS T
	USING(InventoryID)
    GROUP BY Store
)
SELECT 
	Store, Profit, LateArrival_OOSL, LateArrival_OOSL_Profit_Perc,
    AtOrAboveReorder, ROUND(AtOrAboveReorder/(AtOrAboveReorder+BelowReorder),2) AS AtOrAboveReorder_Proportion, 
    AtOrAboveReorderWithStockout, AtOrAboveReorderNoStockout, 
    BelowReorder, ROUND(BelowReorder/(AtOrAboveReorder+BelowReorder),2) AS BelowReorder_Proportion,
    BelowReorderWithStockout, OrderAfterStockout, BelowReorderNoStockout
    
FROM LateArrivalOOSL
INNER JOIN ReorderDetails
USING(Store);

-- -------------------
-- Observations
-- -------------------

-- Most stores exhibit high-risk inventory behavior, frequently placing orders below reorder points. 
-- Store 1 is a of critical concern, leading in both below ROP orders (89%) and significant profit loss from stockouts 147 K. (12.16% of total profit).
-- Store 5 has comparatively lower proportion of orders that are below ROP, but has the highest amount of profit lost about 155 K. (9.35% of total profit). 

-- The following query analyzes vendor performance, specifically lead time average, its standard deviation, and stockout loss associated with each vendor.
WITH AverageLeadTime AS (
	SELECT VendorName, AVG(DATEDIFF(ReceivingDate, PODate)) AS AvgLeadTime
	FROM purchases
	GROUP BY VendorName
),
LeadTimeStdDev AS (
	SELECT VendorName, AvgLeadTime, 
		SQRT(SUM(POW(AvgLeadTime - DATEDIFF(ReceivingDate, PODate), 2)) / COUNT(*)) AS LeadTimeStdDev
	FROM purchases
	INNER JOIN AverageLeadTime
	USING(VendorName)
	GROUP BY VendorName
)
SELECT 
	VendorName, 
    ROUND(AvgLeadTime,0) AS AvgLeadTime, ROUND(LeadTimeStdDev,1) AS LeadTimeStdDev, 
    ROUND(LeadTimeStdDev/AvgLeadTime,2) AS CoefficientOfVariation
FROM LeadTimeStdDev;

-- -------------------
-- Observations
-- -------------------

-- 'VENDOR_008' and 'VENDOR_006' are highly inconsistent in their delivery times having a coefficient of variation of 0.78
-- 'VENDOR_004' and 'VENDOR_010' are comparatively very reliable with coefficient of variation 0.34 and 0.36 respectively.
  
