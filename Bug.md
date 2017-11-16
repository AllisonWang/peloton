##### Segmentation fault when optimize query with GROUP BY and  ORDER BY
In the optimizer_sql_test, when running the query ```SELECT a FROM test GROUP BY a,b ORDER BY b,a, a+b```, the optimizer encouters a segmentation fault 
when choosing the best plan (```Optimizer::ChooseBestPlan```) for group id 1 with op ```PhysicalSortGroupBy``` when calling ```gexpr->GetInputProperties(requirements)``` because the desired 
requirements can not be found in the ```lowest_cost_table_```. I think why the change of cost can lead to this break is that originally, the best expression chosen for 
this group is ```PhysicalHashGroupBy```.
