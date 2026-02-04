# exchange-rate-updater
# 沪港通汇率自动更新程序

这是一个自动更新沪港通结算汇率到飞书表格的程序。

## 功能特点
- 每天自动获取沪港通结算汇率和参考汇率
- 优先使用结算汇率，缺失时使用参考汇率
- 自动识别新增数据，避免重复更新
- 通过GitHub Actions实现定时自动化

## 使用方法
1. 在飞书开放平台创建应用
2. 在GitHub仓库的Settings中设置Secrets
3. 程序会在每天0点（北京时间）自动运行

## 依赖库
- akshare
- pandas
- requests
