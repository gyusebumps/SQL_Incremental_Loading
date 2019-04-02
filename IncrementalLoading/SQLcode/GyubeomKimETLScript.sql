--*************************************************************************--
-- Title: Assignment03
-- Author: <GyubeomKim>
-- Desc: This file tests you knowlege on how to create a Incremental ETL process with SQL code
-- Change Log: When,Who,What
-- 2018-08-02,<GyubeomKim>,Created File
-- 2018-08-02,<GyubeomKim>,added error messages

-- Instructions: 
-- (STEP 1) Create a lite version of the Northwind database by running the provided code.
-- (STEP 2) Create a new Data Warehouse called DWNorthwindLite_withSCD based on the NorthwindLite DB.
--          The DW should have three dimension tables (for Customers, Products, and Dates) and one fact table.
-- (STEP 3) Fill the DW by creating an ETL Script
--**************************************************************************--
USE [DWNorthwindLite_withSCD];
go
SET NoCount ON;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimProducts')
   Drop View vETLDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncDimProducts')
   Drop Procedure pETLSyncDimProducts;
go
	If Exists(Select * from Sys.objects where Name = 'vETLDimCustomers')
   Drop View vETLDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncDimCustomers')
   Drop Procedure pETLSyncDimCustomers;
go
	If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go
	If Exists(Select * from Sys.objects where Name = 'vETLFactOrders')
   Drop View vETLFactOrders;
go
	If Exists(Select * from Sys.objects where Name = 'pETLSyncFactOrders')
   Drop Procedure pETLSyncFactOrders;

--********************************************************************--
-- A) NOT NEEDED FOR INCREMENTAL LOADING: 
 --   Drop the FOREIGN KEY CONSTRAINTS and Clear the tables
--********************************************************************--

--********************************************************************--
-- B) Synchronize the Tables
--********************************************************************--

/****** [dbo].[DimProducts] ******/
go 
Create View vETLDimProducts
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimProducts
** Change Log: When,Who,What
** 2018-08-02,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [ProductID] = p.ProductID
   ,[ProductName] = CAST(p.ProductName as nVarchar(100))
   ,[ProductCategoryID] = p.CategoryID
   ,[ProductCategoryName] = CAST(c.CategoryName as nVarchar(100))
  FROM [NorthwindLite].dbo.Categories as c
  INNER JOIN [NorthwindLite].dbo.Products as p
  ON c.CategoryID = p.CategoryID;
go
/* Testing Code:
 Select * From vETLDimProducts;
*/

go
Create Procedure pETLSyncDimProducts
/* Author: <GyubeomKim>
** Desc: Updates data in DimProducts using the vETLDimProducts view
** Change Log: When,Who,What
** 2018-08-02,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
	With ChangedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
			Except
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
    )UPDATE [DWNorthwindLite_withSCD].dbo.DimProducts 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE ProductID IN (Select ProductID From ChangedProducts)
    ;

    -- 2)For INSERT or UPDATES: Add new rows to the table
	With AddedORChangedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
			Except
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
		)INSERT INTO [DWNorthwindLite_withSCD].dbo.DimProducts
      ([ProductID],[ProductName],[ProductCategoryID],[ProductCategoryName],[StartDate],[EndDate],[IsCurrent])
      SELECT
        [ProductID]
       ,[ProductName]
       ,[ProductCategoryID]
       ,[ProductCategoryName]
       ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
       ,[EndDate] = Null
       ,[IsCurrent] = 1
      FROM vETLDimProducts
      WHERE ProductID IN (Select ProductID From AddedORChangedProducts)
    ;

    -- 3) For Delete: Change the IsCurrent status to zero
    With DeletedProducts 
		As(
			Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From DimProducts
       Where IsCurrent = 1 -- We do not care about row already marked zero!
 			Except            			
      Select ProductID, ProductName, ProductCategoryID, ProductCategoryName From vETLDimProducts
   	)UPDATE [DWNorthwindLite_withSCD].dbo.DimProducts 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE ProductID IN (Select ProductID From DeletedProducts)
   ;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimProducts;
 Print @Status;
 Select * From DimProducts Order By ProductID
*/


/****** [dbo].[DimCustomers] ******/
go 
Create View vETLDimCustomers
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for DimCustomers
** Change Log: When,Who,What
** 2018-08-02,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [CustomerID] = CAST(cu.CustomerID as nchar(5))
   ,[CustomerName] = CAST(cu.CompanyName as nVarchar(100))
   ,[CustomerCity] = CAST(cu.City as nVarchar(100))
   ,[CustomerCountry] = CAST(cu.Country as nVarchar(100))
  FROM [NorthwindLite].dbo.Customers as cu
go
/* Testing Code:
 Select * From vETLDimCustomers;
*/

go
Create Procedure pETLSyncDimCustomers
/* Author: <GyubeomKim>
** Desc: Inserts data into DimCustomers
** Change Log: When,Who,What
** 2018-08-02,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
	With ChangedCustomers 
		As(
			Select [CustomerID], [CustomerName], [CustomerCity], [CustomerCountry] From vETLDimCustomers
			Except
			Select [CustomerID], [CustomerName], [CustomerCity], [CustomerCountry] From DimCustomers
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
    )UPDATE [DWNorthwindLite_withSCD].dbo.DimCustomers 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE CustomerID IN (Select CustomerID From ChangedCustomers)
    ;

    -- 2)For INSERT or UPDATES: Add new rows to the table
	With AddedORChangedCustomers 
		As(
			Select [CustomerID], [CustomerName], [CustomerCity], [CustomerCountry] From vETLDimCustomers
			Except
			Select [CustomerID], [CustomerName], [CustomerCity], [CustomerCountry] From DimCustomers
       Where IsCurrent = 1 -- Needed if the value is changed back to previous value
		)INSERT INTO [DWNorthwindLite_withSCD].dbo.DimCustomers
      ([CustomerID], [CustomerName], [CustomerCity], [CustomerCountry], [StartDate], [EndDate], [IsCurrent])
      SELECT
        [CustomerID]
       ,[CustomerName]
       ,[CustomerCity]
       ,[CustomerCountry]
       ,[StartDate] = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
       ,[EndDate] = Null
       ,[IsCurrent] = 1
      FROM vETLDimCustomers
      WHERE CustomerID IN (Select CustomerID From AddedORChangedCustomers)
    ;

    -- 3) For Delete: Change the IsCurrent status to zero
    With DeletedCustomers 
		As(
			Select [CustomerID], [CustomerName], [CustomerCity], [CustomerCountry] From DimCustomers
       Where IsCurrent = 1 -- We do not care about row already marked zero!
 			Except            			
      Select [CustomerID], [CustomerName], [CustomerCity], [CustomerCountry] From vETLDimCustomers
   	)UPDATE [DWNorthwindLite_withSCD].dbo.DimCustomers 
      SET EndDate = Cast(Convert(nvarchar(50), GetDate(), 112) as int)
         ,IsCurrent = 0
       WHERE CustomerID IN (Select CustomerID From DeletedCustomers)
   ;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncDimCustomers;
 Print @Status;
*/
go

/*Testing Code:
 Declare @Status int;
 Exec @Status = pETLDropForeignKeyConstraints;
 Print @Status;
*/
go
/****** [dbo].[DimDates] ******/
Create Procedure pETLFillDimDates
/* Author: <GyubeomKim>
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 2018-08-02,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code -- 
      ALTER TABLE DWNorthwindLite_withSCD.dbo.FactOrders
      Drop CONSTRAINT fkFactOrdersToDimDates
	  Delete From DimDates; -- Clears table data with the need for dropping FKs
      Declare @StartDate datetime = '01/01/1990'
      Declare @EndDate datetime = '12/31/1999' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
       Insert Into DimDates 
       ( [DateKey], [USADateName], [MonthKey], [MonthName], [QuarterKey], [QuarterName], [YearKey], [YearName] )
       Values ( 
         Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthKey]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Cast(DateName(YYYY,@DateInProcess) + '0' + (DateName(quarter, @DateInProcess) ) as int)  -- [QuarterKey]
        ,'Q' + DateName(quarter, @DateInProcess) + ' - ' + Cast( Year(@DateInProcess) as nVarchar(50) ) -- [QuarterName] 
        ,Year(@DateInProcess) -- [YearKey] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
	   ALTER TABLE DWNorthwindLite_withSCD.dbo.FactOrders
       ADD CONSTRAINT fkFactOrdersToDimDates 
       FOREIGN KEY (OrderDateKey) REFERENCES DimDates(DateKey)
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
*/
go

/****** [dbo].[FactOrders] ******/
go 
Create View vETLFactOrders
/* Author: <GyubeomKim>
** Desc: Extracts and transforms data for FactOrders
** Change Log: When,Who,What
** 2018-08-02,<GyubeomKim>,Created Sproc.
*/
As
  SELECT
    [OrderID] = Cast(o.OrderID as int)
   ,[CustomerKey] = Cast(dc.CustomerKey as int)
   ,[OrderDateKey] = Cast(dd.DateKey as int)
   ,[ProductKey] = Cast(dp.ProductKey as int)
   ,[ActualOrderUnitPrice] = Cast(od.UnitPrice as money)
   ,[ActualOrderQuantity] = Cast(od.Quantity as int)
  FROM [NorthwindLite].dbo.OrderDetails as od
  JOIN [NorthwindLite].dbo.Orders as o
  ON od.OrderID = o.OrderID
  JOIN [DWNorthwindLite].dbo.DimCustomers as dc
  On dc.CustomerID = o.CustomerID
  JOIN [DWNorthwindLite].dbo.DimProducts as dp
  On od.ProductID = dp.ProductID
  JOIN [DWNorthwindLite].dbo.DimDates as dd
  On Cast(Convert(nVarchar(50), o.OrderDate, 112) as int) = dd.DateKey;
go
/* Testing Code:
 Select * From vETLFactOrders;
*/
go
Create Procedure pETLSyncFactOrders
/* Author: <GyubeomKim>
** Desc: Inserts data into FactOrders
** Change Log: When,Who,What
** 2018-08-02,<GyubeomKim>,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
  MERGE INTO FactOrders AS [Target]
      USING vETLFactOrders AS [Source]
      ON [Target].[OrderID] = [Source].[OrderID]
	  And [Target].[CustomerKey] = [Source].[CustomerKey]
	  And [Target].[OrderDateKey] = [Source].[OrderDateKey]
      And [Target].[ProductKey] = [Source].[ProductKey]
      WHEN NOT MATCHED BY TARGET 
      THEN INSERT VALUES
      (       [Source].[OrderID]
            , [Source].[CustomerKey]
            , [Source].[OrderDateKey]
            , [Source].[ProductKey]
            , [Source].[ActualOrderUnitPrice]
			, [Source].[ActualOrderQuantity]
      )      
	  WHEN MATCHED AND 
      (
            [Target].[ActualOrderUnitPrice] <> [Source].[ActualOrderUnitPrice]
		 OR [Target].[ActualOrderQuantity] <> [Source].[ActualOrderQuantity]
      )
      THEN UPDATE SET 
            [Target].[ActualOrderUnitPrice] = [Source].[ActualOrderUnitPrice]
		  , [Target].[ActualOrderQuantity] = [Source].[ActualOrderQuantity]
      WHEN NOT MATCHED BY SOURCE 
		THEN 
			DELETE 
	  ;
  Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLSyncFactOrders;
 Print @Status;
*/
go

--********************************************************************--
-- C)  NOT NEEDED FOR INCREMENTAL LOADING: Re-Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--

--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
go
Declare @Status int = 0;
Exec @Status = pETLSyncDimProducts;
Select [Object] = 'pETLSyncDimProducts', [Status] = Case @Status
	  When +1 Then 'ETL to Sync tables successful!'
	  When -1 Then 'ETL to Sync tables failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLSyncDimCustomers;
Select [Object] = 'pETLSyncDimCustomers', [Status] = Case @Status
	  When +1 Then 'ETL to Sync tables successful!'
	  When -1 Then 'ETL to Sync tables failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLFillDimDates;
Select [Object] = 'pETLFillDimDates', [Status] = Case @Status
	  When +1 Then 'ETL to Fill DimDates table was successful!'
	  When -1 Then 'ETL to Fill DimDates table failed! Common Issues: Tables missing'
	  End

Exec @Status = pETLSyncFactOrders;
Select [Object] = 'pETLSyncFactOrders', [Status] = Case @Status
	  When +1 Then 'ETL to Sync tables successful!'
	  When -1 Then 'ETL to Sync tables failed! Common Issues: Tables missing'
	  End

--Dimproduct Table Check--
Insert Into [NorthwindLite].dbo.Products
Values('Insert Test', 2)
Exec @Status = pETLSyncDimProducts;
Select [Object] = 'pETLSyncDimProducts', [Status] = Case @Status
	  When +1 Then 'Insert successful!'
	  When -1 Then 'Insert failed!'
	  End

Update [NorthwindLite].dbo.Products
SET ProductName = 'Update Test'
Where ProductName = 'Insert Test'
Exec @Status = pETLSyncDimProducts;
Select [Object] = 'pETLSyncDimProducts', [Status] = Case @Status
	  When +1 Then 'Update successful!'
	  When -1 Then 'Update failed!'
	  End

Delete [NorthwindLite].dbo.Products
Where ProductName = 'Update Test'
Exec @Status = pETLSyncDimProducts;
Select [Object] = 'pETLSyncDimProducts', [Status] = Case @Status
	  When +1 Then 'Delete successful!'
	  When -1 Then 'Delete failed!'
	  End

--DimCustomer Table Check--
Insert Into [NorthwindLite].dbo.Customers
Values('a','Insert Test','c','d','e','f')
Exec @Status = pETLSyncDimCustomers;
Select [Object] = 'pETLSyncDimCustomers', [Status] = Case @Status
	  When +1 Then 'Insert successful!'
	  When -1 Then 'Insert failed!'
	  End

Update [NorthwindLite].dbo.Customers
SET CompanyName = 'Update Test'
Where CustomerID = 'a'
Exec @Status = pETLSyncDimCustomers;
Select [Object] = 'pETLSyncDimCustomers', [Status] = Case @Status
	  When +1 Then 'Update successful!'
	  When -1 Then 'Update failed!'
	  End

Delete [NorthwindLite].dbo.Customers
Where CustomerID = 'a'
Exec @Status = pETLSyncDimCustomers;
Select [Object] = 'pETLSyncDimCustomers', [Status] = Case @Status
	  When +1 Then 'Delete successful!'
	  When -1 Then 'Delete failed!'
	  End
--FactOrder Table Check--
Insert Into [NorthwindLite].dbo.OrderDetails
Values(11077, 76, 15.00,2)
Exec @Status = pETLSyncFactOrders;
Select [Object] = 'pETLSyncFactOrders', [Status] = Case @Status
	  When +1 Then 'Insert successful!'
	  When -1 Then 'Insert failed!'
	  End

Update[NorthwindLite].dbo.OrderDetails
Set Quantity = 100
Where ProductID = 76
Exec @Status = pETLSyncFactOrders;
Select [Object] = 'pETLSyncFactOrders', [Status] = Case @Status
	  When +1 Then 'Update successful!'
	  When -1 Then 'Update failed!'
	  End

Delete [NorthwindLite].dbo.OrderDetails
Where ProductID = 76
Exec @Status = pETLSyncFactOrders;
Select [Object] = 'pETLSyncFactOrders', [Status] = Case @Status
	  When +1 Then 'Delete successful!'
	  When -1 Then 'Delete failed!'
	  End
go
Select * from [dbo].[DimProducts];
Select * from [dbo].[DimCustomers];
Select * from [dbo].[DimDates];
Select * from [dbo].[FactOrders];