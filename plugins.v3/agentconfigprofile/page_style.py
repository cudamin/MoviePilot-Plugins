"""
MP Switch 详情页的局部样式，颜色跟随宿主主题，布局跟随插件容器宽度。
"""

# 仅包含固定 CSS，不拼接配置名、错误文本、密钥或其他用户输入。
PAGE_STYLE = """
.mp-switch {
  --ms-ink: rgb(var(--v-theme-on-surface));
  --ms-muted: rgba(var(--v-theme-on-surface), .64);
  --ms-line: rgba(var(--v-theme-on-surface), .12);
  --ms-soft: rgba(var(--v-theme-on-surface), .035);
  --ms-accent: rgb(var(--v-theme-primary));
  --ms-direct-rgb: 124, 86, 210;
  --ms-route-rgb: 5, 123, 137;
  --ms-stack-rgb: 45, 103, 204;
  --ms-direct: rgb(var(--ms-direct-rgb));
  --ms-route: rgb(var(--ms-route-rgb));
  --ms-stack: rgb(var(--ms-stack-rgb));
  color: var(--ms-ink); width: 100%; min-width: 0;
  container: mp-switch / inline-size; font-size: 14px; line-height: 1.6;
}
.mp-switch *, .mp-switch *::before, .mp-switch *::after { box-sizing: border-box; }
.mp-switch h2, .mp-switch h3, .mp-switch p { margin: 0; }
.mp-switch .ms-wrap { min-width: 0; overflow-wrap: anywhere; }
.mp-switch .ms-muted { color: var(--ms-muted); font-size: 12px; }
.mp-switch .ms-header, .mp-switch .ms-row, .mp-switch .ms-actions,
.mp-switch .ms-identity, .mp-switch .ms-section-heading { display: flex; align-items: center; gap: 12px; }
.mp-switch .ms-header { justify-content: space-between; flex-wrap: wrap; padding: 2px 0 24px; gap: 16px; }
.mp-switch .ms-brand { width: 42px; height: 42px; border-radius: 12px; background: var(--ms-accent);
  display: grid; place-items: center; color: rgb(var(--v-theme-on-primary)); flex: 0 0 auto; font-size: 27px; }
.mp-switch .ms-title { font-size: 23px; font-weight: 650; letter-spacing: -.6px; }
.mp-switch .ms-identity { min-width: 0; }
.mp-switch .ms-actions { flex-wrap: wrap; gap: 8px; }
.mp-switch .v-btn { text-transform: none; letter-spacing: 0; font-weight: 500; }
.mp-switch .v-btn:not(.v-btn--icon) { max-width: 100%; }
.mp-switch .v-btn__content { min-width: 0; }
.mp-switch .ms-button .v-btn__content { white-space: normal; overflow-wrap: anywhere; }
.v-theme--dark .mp-switch { --ms-direct-rgb: 190, 167, 255; --ms-route-rgb: 89, 210, 217;
  --ms-stack-rgb: 123, 173, 255; }
.mp-switch .ms-mode-direct { --ms-mode-rgb: var(--ms-direct-rgb); --ms-mode: var(--ms-direct); }
.mp-switch .ms-mode-route { --ms-mode-rgb: var(--ms-route-rgb); --ms-mode: var(--ms-route); }
.mp-switch .ms-mode-stack { --ms-mode-rgb: var(--ms-stack-rgb); --ms-mode: var(--ms-stack); }
.mp-switch .ms-mode-row { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
  gap: 12px; margin: 0 0 18px; }
.mp-switch .ms-mode-tabs { display: inline-flex; gap: 2px; padding: 3px; border-radius: 10px;
  background: rgba(var(--v-theme-on-surface), .065); }
.mp-switch .ms-mode-tab.v-btn { min-width: 94px; height: 32px; padding: 0 13px; border-radius: 7px;
  color: var(--ms-muted); font-size: 13px; font-weight: 500; }
.mp-switch .ms-mode-tab .v-btn__content { gap: 7px; }
.mp-switch .ms-mode-tab.is-selected { color: var(--ms-ink); background: rgb(var(--v-theme-surface));
  box-shadow: 0 1px 3px rgba(0, 0, 0, .1); font-weight: 600; }
.mp-switch .ms-mode-tab.is-current { color: var(--ms-mode); font-weight: 600; }
.mp-switch .ms-mode-dot { display: inline-block; width: 6px; height: 6px; border-radius: 50%;
  background: var(--ms-mode); flex: 0 0 auto; }
.mp-switch .ms-mode-controls { display: flex; align-items: center; gap: 8px; }
.mp-switch .ms-failover.v-btn { color: var(--ms-muted); padding: 0 8px; }
.mp-switch .ms-failover.is-on { color: var(--ms-route); }
.mp-switch .ms-failover .v-btn__append .v-icon { font-size: 30px; }
.mp-switch .ms-activation { display: flex; align-items: center; justify-content: space-between;
  flex-wrap: wrap; gap: 14px; padding: 14px 18px; margin-top: 14px; border-radius: 10px;
  background: rgba(var(--ms-mode-rgb), .07); border: 1px solid rgba(var(--ms-mode-rgb), .17); }
.mp-switch .ms-activation-title { color: var(--ms-mode); font-weight: 600; font-size: 13px; }
.mp-switch .ms-mode-activate.v-btn { background: var(--ms-mode); color: rgb(var(--v-theme-surface)); }
.mp-switch .ms-mode-icon { display: inline-flex; align-items: center; margin-right: 6px; }
.mp-switch .ms-current { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 20px;
  padding: 22px 24px; border: 1px solid var(--ms-line); border-radius: 14px; background: var(--ms-soft); }
.mp-switch .ms-current h2 { font-size: 23px; line-height: 1.35; font-weight: 600; margin: 8px 0; }
.mp-switch .ms-current-label { display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--ms-muted); }
.mp-switch .ms-dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%;
  background: rgb(var(--v-theme-success)); flex: 0 0 auto; }
.mp-switch .ms-current-side { display: flex; flex-direction: column; justify-content: center;
  align-items: flex-end; gap: 10px; text-align: right; }
.mp-switch .ms-main { display: flex; flex-direction: column; gap: 22px; margin-top: 24px; }
.mp-switch .ms-section-heading { justify-content: space-between; flex-wrap: wrap; margin-bottom: 14px; }
.mp-switch .ms-section-heading h2 { font-size: 16px; font-weight: 600; }
.mp-switch .ms-section-heading h3 { font-size: 14px; font-weight: 600; }
.mp-switch .ms-provider-list { display: flex; flex-direction: column; gap: 10px; }
.mp-switch .ms-provider { border: 1px solid var(--ms-line); border-radius: 12px; padding: 18px; min-width: 0; }
.mp-switch .ms-provider.is-active { border-color: rgba(var(--ms-mode-rgb), .6);
  background: rgba(var(--ms-mode-rgb), .045); }
.mp-switch .ms-provider-head { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 12px; }
.mp-switch .ms-avatar { width: 38px; height: 38px; display: grid; place-items: center; border-radius: 10px;
  background: rgba(var(--v-theme-primary), .1); color: var(--ms-accent); font-size: 17px; font-weight: 600; }
.mp-switch .ms-provider-title { display: flex; flex-wrap: wrap; align-items: baseline; gap: 7px; min-width: 0; }
.mp-switch .ms-provider-title h3 { font-size: 15px; font-weight: 600; min-width: 0;
  overflow-wrap: anywhere; max-width: 100%; }
.mp-switch .ms-model { font-size: 13px; margin: 4px 0 2px; overflow-wrap: anywhere; }
.mp-switch .ms-provider-bottom { margin-top: 14px; display: flex; align-items: center;
  justify-content: space-between; flex-wrap: wrap; gap: 10px; }
.mp-switch .ms-status { font-size: 12px; display: inline-flex; align-items: center; gap: 6px; min-width: 0; }
.mp-switch .ms-status::before { content: ''; width: 6px; height: 6px; border-radius: 50%;
  background: currentColor; flex: 0 0 auto; }
.mp-switch .ms-tone-success { color: rgb(var(--v-theme-success)); }
.mp-switch .ms-tone-error { color: rgb(var(--v-theme-error)); }
.mp-switch .ms-tone-warning { color: rgb(var(--v-theme-warning)); }
.mp-switch .ms-tone-muted { color: var(--ms-muted); }
.v-theme--light .mp-switch .ms-tone-success { color: #28723c; }
.v-theme--light .mp-switch .ms-tone-error { color: #bd293c; }
.v-theme--light .mp-switch .ms-tone-warning { color: #8b5d14; }
.mp-switch .ms-tag { display: inline-flex; align-items: center; padding: 1px 7px; border-radius: 5px;
  font-size: 11px; line-height: 1.7; background: var(--ms-soft); color: var(--ms-muted); }
.mp-switch .ms-tag-primary { color: var(--ms-accent); background: rgba(var(--v-theme-primary), .1); }
.mp-switch .ms-more { margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--ms-line); }
.mp-switch summary { cursor: pointer; width: fit-content; max-width: 100%; color: var(--ms-muted); font-size: 12px; }
.mp-switch summary:focus-visible, .mp-switch a:focus-visible { outline: 2px solid var(--ms-accent); outline-offset: 4px; }
.mp-switch .ms-more[open] summary { margin-bottom: 12px; }
.mp-switch .ms-more-body { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.mp-switch .ms-aside-section { min-width: 0; }
.mp-switch .ms-aside-section h2 { font-size: 15px; font-weight: 600; margin-bottom: 3px; }
.mp-switch .ms-rule { display: flex; justify-content: space-between; gap: 12px; padding: 9px 0;
  border-bottom: 1px solid var(--ms-line); font-size: 12px; }
.mp-switch .ms-rule-value { text-align: right; color: var(--ms-ink); overflow-wrap: anywhere; }
.mp-switch .ms-queue { list-style: none; padding: 0; margin: 16px 0 22px; }
.mp-switch .ms-queue li { display: flex; gap: 10px; align-items: flex-start; padding: 8px 0; min-width: 0; }
.mp-switch .ms-queue-index { font-size: 11px; width: 22px; height: 22px; display: grid;
  place-items: center; border: 1px solid var(--ms-line); border-radius: 50%; flex: 0 0 auto; }
.mp-switch .ms-queue-name { font-size: 12px; overflow-wrap: anywhere; min-width: 0; }
.mp-switch .ms-aside-section + .ms-aside-section { margin-top: 24px; }
.mp-switch .ms-catalog { margin-top: 24px; padding-top: 20px; border-top: 1px solid var(--ms-line); }
.mp-switch .ms-catalog-row { display: flex; justify-content: space-between; align-items: center;
  gap: 14px; padding: 12px 0; border-bottom: 1px solid var(--ms-line); }
.mp-switch .ms-catalog-name { font-size: 13px; font-weight: 500; }
.mp-switch .ms-empty { text-align: center; padding: 36px 20px; border: 1px dashed var(--ms-line); border-radius: 12px; }
.mp-switch .ms-empty-title { font-size: 16px; font-weight: 600; margin-bottom: 6px; }
.mp-switch .ms-pagination { display: flex; align-items: center; justify-content: space-between;
  gap: 8px; margin-top: 16px; font-size: 12px; color: var(--ms-muted); }
.mp-switch .ms-disclosure { margin-top: 24px; padding-top: 20px; border-top: 1px solid var(--ms-line); }
.mp-switch .ms-disclosure > summary { font-size: 15px; font-weight: 600; color: var(--ms-ink); }
.mp-switch .ms-disclosure[open] > summary { margin-bottom: 18px; }
.mp-switch .ms-model-group { padding: 14px 0; }
.mp-switch .ms-model-group + .ms-model-group { border-top: 1px solid var(--ms-line); }
.mp-switch .ms-log { padding: 10px 0; display: grid; grid-template-columns: 138px minmax(0, 1fr);
  gap: 14px; border-bottom: 1px solid var(--ms-line); font-size: 12px; }
.mp-switch .ms-progress { margin-top: 16px; padding: 14px 18px; border: 1px solid var(--ms-line); border-radius: 10px; }
.mp-switch .ms-notice { margin: 0 0 16px; font-size: 13px; }
.mp-switch .ms-footer { display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px;
  padding-top: 22px; margin-top: 22px; border-top: 1px solid var(--ms-line); font-size: 11px; color: var(--ms-muted); }
.mp-switch .ms-footer a { color: inherit; text-decoration: underline; text-underline-offset: 3px; }
@container mp-switch (max-width: 520px) {
  .mp-switch .ms-title { font-size: 21px; }
  .mp-switch .ms-header { padding-bottom: 18px; }
  .mp-switch .ms-current { grid-template-columns: minmax(0, 1fr); padding: 18px; gap: 14px; }
  .mp-switch .ms-current h2 { font-size: 21px; }
  .mp-switch .ms-current-side { align-items: flex-start; text-align: left; }
  .mp-switch .ms-provider { padding: 14px; }
  .mp-switch .ms-provider-bottom { align-items: flex-start; }
  .mp-switch .ms-provider-bottom > .ms-actions { width: 100%; gap: 5px; }
  .mp-switch .ms-mode-tabs { width: 100%; }
  .mp-switch .ms-mode-tab.v-btn { min-width: 0; flex: 1; padding: 0 8px; }
  .mp-switch .ms-catalog-row { flex-wrap: wrap; gap: 8px; }
  .mp-switch .ms-log { grid-template-columns: minmax(0, 1fr); gap: 2px; }
}
"""
