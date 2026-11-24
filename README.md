# Shating Lake archive

标准库 + SQLite 实现的 `lake.Lake(目录)`。热账 (`hot.sqlite3`) 可由备份恢复；
目录元数据 (`catalog.sqlite3`) 与长期恢复正文卷 `S/`、`C/` 不随热账回滚。

## 公开接口

```python
from lake import Lake

lake = Lake("some-directory")
b = lake.freeze("batch", [("A", 7), ("B", None), ("C", 11)])
lake.backup("pre-seal")          # 可在 seal 前备份
a = lake.admit(b, [("A", 7), ("B", None), ("C", 11)])
r = lake.seal(a)
g = lake.protect("pre-seal", r)

permit, already_deleted = lake.permit("q", r, "S")
lake.purge(permit)               # 实际删除 S 卷

lake.restore("pre-seal", g, r, hook)
print(lake.read(r))              # (r, [("A", 7), ...], 18)

lake.release(g)
# S、C 两卷都删完后：(r, "已到期")
```

`restore` 会在首次选中合格卷后、发布恢复结果前调用一次 `hook(v)`。回调中可用
另一个 `Lake` 实例执行同参重开、发证和清退；若另一卷仍合格，恢复会改选该卷并
成功。绑定错配、逐行不符或合格证据不足时，整次操作拒绝且不替换热账。

## 运行

```sh
python3 -B prepare.py
python3 -B verify.py
python3 -B demo.py
```

`.pointwise-runtime.json` 已分别配置 `setup`、`verify` 和 `demo` 的 argv 数组。
