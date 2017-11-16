##### Segmentation fault when optimize query with GROUP BY and  ORDER BY
In the optimizer_sql_test, when running the query ```SELECT a FROM test GROUP BY a,b ORDER BY b,a, a+b```, the optimizer encouters a segmentation fault 
when choosing the best plan (```Optimizer::ChooseBestPlan```) for group id 1 with op ```PhysicalSortGroupBy``` when calling ```gexpr->GetInputProperties(requirements)``` because the desired 
requirements can not be found in the ```lowest_cost_table_```. I think why the change of cost can lead to this break is that originally, the best expression chosen for 
this group is ```PhysicalHashGroupBy```.

After I changed one line in optimizer.cpp from ```if (output_properties >= requirements)``` to ```if (!(requirements >= output_properties)) ```, the segmentation fault can be avoilded while a new error occurs when executing the plan with error message ```Message :: Attribute '' is not an available attribute``` due to the change of the generated plan which is originally shown as
```
Plan Type: ORDERBY
  OrderBy
NumChildren: 1Plan Type: AGGREGATE_V2
    AggregatePlan
NumChildren: 1Plan Type: SEQSCAN
      SeqScan
NumChildren: 0
```
Now, it becomes 
```
Plan Type: AGGREGATE_V2
  AggregatePlan
NumChildren: 1Plan Type: ORDERBY
    OrderBy
NumChildren: 1Plan Type: PROJECTION
      Projection
NumChildren: 1Plan Type: SEQSCAN
        SeqScan
NumChildren: 0
```
