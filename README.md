一个未完成的因子检测工具
-----------------------------------
使用回归法分析因子
----------------------------------
1. 使用MAD法（5）处理异常值
2. 使用均值方差归一法标准化因子值
3. 空值填充0
4. 算因子的截面收益率（加入控制变量industry dummies和ln market cap）
5. beta（因子收益率序列）和se序列，计算得到t值序列
6. 分析tvalue_abs_mean,tvalue_mean,tvalue>2,beta_mean和beta_cumsum
