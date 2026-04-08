/**
 * PROCARE — Sync & Upload buttons
 * Include on any page: <script src="sync.js"></script>
 * Place <div id="syncArea"></div> in the header nav area.
 *
 * Provides:
 * - Sync button → triggers fetch.yml workflow
 * - Upload COG button → triggers upload_cog.yml workflow
 * - Upload XLS button → uploads priceVAT.xls to repo via GitHub Contents API
 */
(function() {
  'use strict';

  var SYNC_CFG = null;

  function loadConfig() {
    if (SYNC_CFG) return Promise.resolve(SYNC_CFG);
    return fetch('config.json').then(function(r) { return r.json(); }).then(function(cfg) {
      SYNC_CFG = cfg;
      return cfg;
    });
  }

  function getGHToken() {
    return localStorage.getItem('procare_gh_token') || '';
  }

  function saveGHToken(token) {
    localStorage.setItem('procare_gh_token', token);
  }

  function askGHToken() {
    var token = getGHToken();
    if (token) return token;
    token = prompt('Введи GitHub Token (Personal Access Token) для запуска workflow:');
    if (token && token.trim()) {
      saveGHToken(token.trim());
      return token.trim();
    }
    return null;
  }

  function triggerWorkflow(workflowFile, inputs) {
    var token = askGHToken();
    if (!token) return;
    loadConfig().then(function(cfg) {
      var url = 'https://api.github.com/repos/' + cfg.repo + '/actions/workflows/' + workflowFile + '/dispatches';
      var body = { ref: 'main' };
      if (inputs) body.inputs = inputs;

      fetch(url, {
        method: 'POST',
        headers: {
          'Authorization': 'token ' + token,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
      }).then(function(resp) {
        if (resp.status === 204) {
          showNotice('success', 'Workflow запущен! Данные обновятся через несколько минут.');
        } else if (resp.status === 401 || resp.status === 403) {
          localStorage.removeItem('procare_gh_token');
          showNotice('error', 'Неверный GitHub Token. Попробуй ещё раз.');
        } else {
          resp.text().then(function(t) {
            showNotice('error', 'Ошибка: HTTP ' + resp.status + ' — ' + t.slice(0, 100));
          });
        }
      }).catch(function(e) {
        showNotice('error', 'Ошибка сети: ' + e.message);
      });
    });
  }

  function uploadXLS() {
    var token = askGHToken();
    if (!token) return;

    var input = document.createElement('input');
    input.type = 'file';
    input.accept = '.xls,.xlsx,.csv';
    input.onchange = function() {
      var file = input.files[0];
      if (!file) return;

      showNotice('info', 'Загрузка файла ' + file.name + '...');

      var reader = new FileReader();
      reader.onload = function() {
        var base64 = btoa(new Uint8Array(reader.result).reduce(function(d, b) {
          return d + String.fromCharCode(b);
        }, ''));

        loadConfig().then(function(cfg) {
          // Check if file already exists (to get SHA for update)
          var apiUrl = 'https://api.github.com/repos/' + cfg.repo + '/contents/priceVAT.xls';
          var headers = {
            'Authorization': 'token ' + token,
            'Accept': 'application/vnd.github.v3+json'
          };

          fetch(apiUrl, { headers: headers }).then(function(r) {
            return r.ok ? r.json() : null;
          }).then(function(existing) {
            var body = {
              message: 'Upload priceVAT.xls ' + new Date().toISOString().slice(0, 10),
              content: base64
            };
            if (existing && existing.sha) {
              body.sha = existing.sha;
            }
            return fetch(apiUrl, {
              method: 'PUT',
              headers: Object.assign({ 'Content-Type': 'application/json' }, headers),
              body: JSON.stringify(body)
            });
          }).then(function(resp) {
            if (resp.ok) {
              showNotice('success', 'Файл загружен! Теперь нажми "Обновить COG" для применения цен.');
            } else {
              resp.text().then(function(t) {
                showNotice('error', 'Ошибка загрузки: ' + t.slice(0, 200));
              });
            }
          }).catch(function(e) {
            showNotice('error', 'Ошибка: ' + e.message);
          });
        });
      };
      reader.readAsArrayBuffer(file);
    };
    input.click();
  }

  function showNotice(type, msg) {
    var el = document.getElementById('syncNotice');
    if (!el) {
      el = document.createElement('div');
      el.id = 'syncNotice';
      el.style.cssText = 'position:fixed;top:16px;right:16px;z-index:9999;max-width:400px;padding:12px 18px;border-radius:10px;font-size:13px;font-weight:500;font-family:inherit;box-shadow:0 4px 20px rgba(0,0,0,0.12);transition:opacity 0.3s;cursor:pointer';
      el.onclick = function() { el.style.display = 'none'; };
      document.body.appendChild(el);
    }
    var colors = {
      success: { bg: '#edfaf5', border: '#0F6E56', color: '#085041' },
      error:   { bg: '#fef0f0', border: '#A32D2D', color: '#791F1F' },
      info:    { bg: '#f0f7ff', border: '#185FA5', color: '#0c3b6e' }
    };
    var c = colors[type] || colors.info;
    el.style.background = c.bg;
    el.style.border = '1px solid ' + c.border;
    el.style.color = c.color;
    el.style.display = 'block';
    el.style.opacity = '1';
    el.textContent = msg;
    clearTimeout(el._timer);
    el._timer = setTimeout(function() { el.style.opacity = '0'; setTimeout(function() { el.style.display = 'none'; }, 300); }, 5000);
  }

  // Render buttons
  function renderSyncButtons() {
    var area = document.getElementById('syncArea');
    if (!area) return;

    area.innerHTML =
      '<button id="btnSync" style="font-size:11px;padding:5px 12px;border-radius:7px;background:#185FA5;color:#fff;border:1px solid #185FA5;cursor:pointer;font-weight:600;font-family:inherit;white-space:nowrap;transition:opacity .15s" title="Запустить сбор данных с Allegro">⟳ Синхронизация</button>' +
      '<button id="btnUploadXLS" style="font-size:11px;padding:5px 12px;border-radius:7px;background:#BA7517;color:#fff;border:1px solid #BA7517;cursor:pointer;font-weight:600;font-family:inherit;margin-left:6px;white-space:nowrap;transition:opacity .15s" title="Загрузить файл с ценами (XLS/XLSX/CSV)">⬆ Загрузить цены</button>' +
      '<button id="btnUpdateCOG" style="font-size:11px;padding:5px 12px;border-radius:7px;background:#0F6E56;color:#fff;border:1px solid #0F6E56;cursor:pointer;font-weight:600;font-family:inherit;margin-left:6px;white-space:nowrap;transition:opacity .15s" title="Применить загруженные цены к products.json">⟳ Обновить COG</button>';

    document.getElementById('btnSync').addEventListener('click', function() {
      triggerWorkflow('fetch.yml');
    });
    document.getElementById('btnUploadXLS').addEventListener('click', function() {
      uploadXLS();
    });
    document.getElementById('btnUpdateCOG').addEventListener('click', function() {
      triggerWorkflow('upload_cog.yml');
    });

    // Hover effects
    ['btnSync', 'btnUploadXLS', 'btnUpdateCOG'].forEach(function(id) {
      var btn = document.getElementById(id);
      if (!btn) return;
      btn.addEventListener('mouseenter', function() { btn.style.opacity = '0.8'; });
      btn.addEventListener('mouseleave', function() { btn.style.opacity = '1'; });
    });
  }

  // Init on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', renderSyncButtons);
  } else {
    renderSyncButtons();
  }
})();
