# 空间清理器使用方法

使用时需在EMBY设置中的通知处配置Webhooks，Events需勾选播放。网址格式：

http://<MP容器IP地址>:3001/api/v1/webhook?token=<MP设置里的API令牌>

<img width="1096" height="540" alt="image" src="https://github.com/user-attachments/assets/2933d154-2e59-4f3e-8716-bbccd28b8710" />
<img width="725" height="463" alt="image" src="https://github.com/user-attachments/assets/8bd4e614-e4ef-4c6d-a9b2-e39fb75bf3e4" />
<img width="759" height="381" alt="image" src="https://github.com/user-attachments/assets/93e84a2b-6e5a-463f-9f6d-580f3650c6ba" />


# 联动删除辅种(同tmdbid标签)
该功能需配合插件种子标签器使用

<img width="347" height="175" alt="image" src="https://github.com/user-attachments/assets/acf8d9e2-9896-4756-a485-0f51679653fd" />

# 同一集只下载一次
R到多个组的时候只下第一个发布的，避免重复下载

# rss下载中的阈值
缓存中观看进度大于阈值的将被跳过下载
