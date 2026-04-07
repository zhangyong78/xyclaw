# OKX 桌面量化程序

这是一个运行在 Windows 上的桌面版 OKX 量化交易程序，支持：

- 本地回测
- OKX 实时信号检查
- OKX 模拟盘交易
- OKX 实盘交易

## 当前内置策略

- 方向：只做多
- 周期：`1小时`
- 参数：`EMA21`、`EMA55`
- 开仓：`EMA21` 上穿 `EMA55` 后做多
- 止损：收盘价跌破 `EMA55` 后止损
- 平仓：`EMA21` 下穿 `EMA55` 后平仓

## 启动方式

正常使用：

```powershell
双击：启动OKX桌面程序.bat
```

查看报错：

```powershell
双击：启动OKX桌面程序_调试.bat
```

## 主要文件

- 桌面程序入口：`app/desktop_main.pyw`
- 桌面界面：`app/desktop_app.py`
- 回测入口：`app/main.py`
- 实盘入口：`app/live_main.py`
- 共享服务层：`app/services.py`

## 依赖安装

```powershell
pip install -e .
```

## 注意事项

- 首次使用建议先做回测
- 接 OKX 前建议先用模拟盘
- 不勾选真实下单时，只做信号检查或模拟预演
- 桌面程序的详细说明见 `桌面版使用说明.md`
