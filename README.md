# 锁序解剖室

从线程事件复盘死锁与近失事件：导入 acquire 请求/成功/释放、线程开始结束、
condition wait/notify 与时间戳，按采集代次保留原始顺序，重建任意时点的
持有图与等待图，检测环路并给出完整证据（线程、锁、持有栈、等待栈）。

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest -q
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 5270
# 打开 http://127.0.0.1:5270 ，点击「载入示例」
```

## 结构

- `locklab/engine.py` — 事件回放引擎：可重入锁、读写锁共享/升级、condition
  暂时释放与重新竞争、超时/线程终止撤销等待；缺事件与乱序产出带
  `candidate` 置信状态的候选图，绝不凭空补 release；锁身份 = (代次, 地址)。
  `Incremental` 按段设检查点，流式追加只重算受影响尾段。
- `locklab/minimize.py` — ddmin 风格缩减：保持每线程 acquire/release 配对与
  condition 语义，把长轨迹缩减为仍复现同一环的最小片段。
- `locklab/db.py` — SQLite 存储：事件按 (branch, epoch, seq) 保留原始顺序；
  校正与锁类型声明形成新分支，并列合法重建都保留。
- `app.py` — FastAPI：导入/流式追加/图快照/时间线/分支比较/最小化/声明。
- `static/index.html` — 页面：线程泳道、等待图时间滑块、环路证据、分支比较。

## 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/epochs/{epoch}/events` | 按代次批量导入事件 |
| POST | `/api/stream?epoch=N` | 流式追加，返回增量重算统计 |
| GET | `/api/graph?branch=&t=` | 时点持有图/等待图/环/异常 |
| GET | `/api/timeline?branch=` | 泳道数据 |
| GET | `/api/compare?a=&b=&t=` | 分支事件与图差异 |
| POST | `/api/branches` | 校正/声明形成新分支 |
| POST | `/api/minimize` | 缩减复现环的最小事件片段 |
| POST | `/api/demo` | 载入示例场景 |
