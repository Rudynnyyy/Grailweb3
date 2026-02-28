# 自动化选币器（静态版）

此文件夹可直接作为一个独立仓库上传到 GitHub（把整个 `app/` 目录内容作为仓库根目录）。

## 功能说明

- 可在浏览器端完成：市场选择、参数调整、内置条件勾选、自定义因子表达式、排序、显示列管理等
- 数据来源：`data/latest.json`（快照文件）

说明：此静态版不包含 Python 后端，因此网页上的“刷新/自动更新”只能重新加载 `latest.json`，无法在线拉取交易所最新数据。

## 本地打开方式

直接双击打开 `index.html` 也能用；更推荐用任意静态服务器（避免浏览器缓存/权限差异），例如：

- VSCode: Live Server
- Python: `python -m http.server 8000`（在此目录下执行）

## Netlify 部署（给别人点点鼠标测试）

1. 新建 GitHub 仓库（仓库根目录就是本 `app/` 的内容）
2. Netlify → Add new site → Import from Git
3. 选择该仓库
4. Build command：留空
5. Publish directory：`.`（仓库根目录）
6. Deploy

## 更新数据的推荐方式（小时级）

静态托管无法运行 Python 后端接口 `/api/refresh`。如果希望线上小时级更新数据：

- 使用 GitHub Actions（或你自己的定时任务）每小时生成新的 `data/latest.json`，提交到仓库
- Netlify 会在仓库更新后自动重新部署，从而实现“定时更新数据”

