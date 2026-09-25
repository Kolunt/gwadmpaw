function getAdminSettingsConfig() {
    const el = document.getElementById('admin-settings-config');
    if (!el) return { urls: {}, csrf_token: '', bot_menu_items: [] };
    return JSON.parse(el.textContent);
}

let botMenuItems = [];

// Переключение табов с поддержкой URL hash
document.addEventListener('DOMContentLoaded', function() {
    const config = getAdminSettingsConfig();
    botMenuItems = config.bot_menu_items || [];
    const tabs = document.querySelectorAll('.settings-tab');
    const tabContents = document.querySelectorAll('.settings-tab-content');
    
    // Функция для переключения таба
    function switchTab(tabName) {
        // Убираем активный класс у всех табов и содержимого
        tabs.forEach(t => t.classList.remove('active'));
        tabContents.forEach(content => content.classList.remove('active'));
        
        // Добавляем активный класс к выбранному табу и содержимому
        const selectedTab = document.querySelector(`.settings-tab[data-tab="${tabName}"]`);
        const selectedContent = document.querySelector(`.settings-tab-content[data-tab="${tabName}"]`);
        
        if (selectedTab && selectedContent) {
            selectedTab.classList.add('active');
            selectedContent.classList.add('active');
            
            // Если переключились на таб "integrations" и hash не указывает на подтаб, активируем подтаб по умолчанию
            if (tabName === 'integrations') {
                const currentHash = window.location.hash;
                if (!currentHash.startsWith('#integrations-')) {
                    // Активируем подтаб по умолчанию (dadata)
                    setTimeout(() => {
                        switchIntegrationTab('dadata');
                    }, 50);
                }
            } else {
            // Обновляем URL hash без перезагрузки страницы
            if (window.location.hash !== '#' + tabName) {
                window.history.replaceState(null, '', '#' + tabName);
                }
            }
        }
    }
    
    // Обработка кликов по табам
    tabs.forEach(tab => {
        tab.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const targetTab = this.getAttribute('data-tab');
            if (targetTab) {
            switchTab(targetTab);
                // Обновляем URL hash
                window.history.replaceState(null, '', '#' + targetTab);
            }
        });
    });
    
    // Функция для обработки hash (основные табы и подтабы интеграций)
    function handleHash() {
    const hash = window.location.hash.substring(1); // Убираем #
        
        // Если нет hash, активируем первый таб
        if (!hash) {
            const firstTab = tabs[0];
            if (firstTab) {
                const firstTabName = firstTab.getAttribute('data-tab');
                switchTab(firstTabName);
            }
            return;
        }
        
        // Проверяем, является ли это подтабом интеграций (формат: integrations-telegram, integrations-smtp, integrations-dadata)
        if (hash.startsWith('integrations-')) {
            const integration = hash.replace('integrations-', '');
            if (['dadata', 'smtp', 'telegram', 'gwars'].includes(integration)) {
                // Сначала активируем таб "integrations"
                switchTab('integrations');
                // Затем активируем подтаб с небольшой задержкой, чтобы убедиться, что таб активирован
                setTimeout(() => {
                    switchIntegrationTab(integration);
                }, 100);
            }
        } else {
            // Обычный таб
        const tabExists = Array.from(tabs).some(t => t.getAttribute('data-tab') === hash);
        if (tabExists) {
            switchTab(hash);
            } else {
                // Если таб не найден, активируем первый таб
                const firstTab = tabs[0];
                if (firstTab) {
                    const firstTabName = firstTab.getAttribute('data-tab');
                    switchTab(firstTabName);
                }
            }
        }
    }
    
    // Переключение подтабов интеграций с поддержкой URL hash
    function switchIntegrationTab(integration) {
        const integrationSubtabs = document.querySelectorAll('.integration-subtab');
        const integrationContents = document.querySelectorAll('.integration-content');
        
        // Убираем активный класс у всех подтабов и содержимого
        integrationSubtabs.forEach(t => t.classList.remove('active'));
        integrationContents.forEach(c => c.classList.remove('active'));
        
        // Добавляем активный класс к выбранному подтабу и содержимому
        const selectedTab = document.querySelector(`.integration-subtab[data-integration="${integration}"]`);
        const selectedContent = document.querySelector(`.integration-content[data-integration="${integration}"]`);
        
        if (selectedTab && selectedContent) {
            selectedTab.classList.add('active');
            selectedContent.classList.add('active');
            
            // Обновляем URL hash (только если мы на табе integrations)
            const currentHash = window.location.hash;
            const newHash = '#integrations-' + integration;
            if (currentHash !== newHash) {
                window.history.replaceState(null, '', newHash);
            }
            
            // Если переключились на Telegram, загружаем меню
            if (integration === 'telegram' && typeof loadBotMenu === 'function') {
                setTimeout(() => {
                    loadBotMenu();
                }, 100);
            }
        }
    }
    
    // Проверяем hash при загрузке страницы
    // Используем небольшую задержку, чтобы убедиться, что DOM полностью загружен
    setTimeout(() => {
        handleHash();
        // Дополнительная проверка: если после handleHash ничего не активировано, активируем первый таб
        const hasActiveTab = document.querySelector('.settings-tab.active');
        const hasActiveContent = document.querySelector('.settings-tab-content.active');
        if (!hasActiveTab || !hasActiveContent) {
            const firstTab = tabs[0];
            if (firstTab) {
                const firstTabName = firstTab.getAttribute('data-tab');
                switchTab(firstTabName);
            }
        }
    }, 100);
    
    // Обработка изменения hash (когда пользователь использует кнопки браузера)
    window.addEventListener('hashchange', handleHash);
    
    // Дополнительная проверка hash после небольшой задержки (на случай, если DOM еще не полностью готов)
    setTimeout(() => {
        const hash = window.location.hash.substring(1);
        if (hash && hash.startsWith('integrations-')) {
            const integration = hash.replace('integrations-', '');
            if (['dadata', 'smtp', 'telegram', 'gwars'].includes(integration)) {
                // Проверяем, что подтаб действительно активен
                const selectedContent = document.querySelector(`.integration-content[data-integration="${integration}"]`);
                if (selectedContent && !selectedContent.classList.contains('active')) {
                    switchTab('integrations');
                    setTimeout(() => {
                        switchIntegrationTab(integration);
                    }, 50);
                }
            }
        }
    }, 200);
    
    const integrationSubtabs = document.querySelectorAll('.integration-subtab');
    const integrationContents = document.querySelectorAll('.integration-content');
    
    integrationSubtabs.forEach(tab => {
        tab.addEventListener('click', function() {
            const integration = this.getAttribute('data-integration');
            switchIntegrationTab(integration);
        });
    });

    const gwarsDomainTable = document.getElementById('gwars-domain-rows');
    const gwarsDomainMapInput = document.getElementById('gwars_domain_map');
    const gwarsAddRowBtn = document.getElementById('gwars-add-row');

    function createGwarsDomainRow(host = '', siteId = '', isPrimary = false) {
        const row = document.createElement('tr');
        row.className = 'gwars-domain-row';
        row.innerHTML = `
            <td><input type="text" class="setting-input gwars-host-input" value="${host}" placeholder="gwadm.ru"></td>
            <td><input type="number" min="1" class="setting-input gwars-site-id-input" value="${siteId}" style="width: 100px;"></td>
            <td style="text-align: center;"><input type="radio" name="gwars_domain_primary_row" class="gwars-primary-radio" ${isPrimary ? 'checked' : ''}></td>
            <td><button type="button" class="btn btn-secondary btn-sm gwars-remove-row">Удалить</button></td>
        `;
        return row;
    }

    function serializeGwarsDomainMap() {
        if (!gwarsDomainTable || !gwarsDomainMapInput) {
            return;
        }
        const rows = gwarsDomainTable.querySelectorAll('.gwars-domain-row');
        const entries = [];
        rows.forEach(row => {
            const host = row.querySelector('.gwars-host-input')?.value.trim().toLowerCase();
            const siteId = parseInt(row.querySelector('.gwars-site-id-input')?.value || '0', 10);
            const isPrimary = row.querySelector('.gwars-primary-radio')?.checked;
            if (!host) {
                return;
            }
            entries.push({ host, site_id: siteId, primary: !!isPrimary });
        });
        gwarsDomainMapInput.value = JSON.stringify(entries);
    }

    if (gwarsAddRowBtn && gwarsDomainTable) {
        gwarsAddRowBtn.addEventListener('click', function() {
            const hasPrimary = gwarsDomainTable.querySelector('.gwars-primary-radio:checked');
            gwarsDomainTable.appendChild(createGwarsDomainRow('', '', !hasPrimary));
        });

        gwarsDomainTable.addEventListener('click', function(event) {
            const removeBtn = event.target.closest('.gwars-remove-row');
            if (!removeBtn) {
                return;
            }
            const row = removeBtn.closest('.gwars-domain-row');
            const wasPrimary = row?.querySelector('.gwars-primary-radio')?.checked;
            row?.remove();
            if (wasPrimary) {
                const firstRadio = gwarsDomainTable.querySelector('.gwars-primary-radio');
                if (firstRadio) {
                    firstRadio.checked = true;
                }
            }
        });
    }

    const settingsForm = document.querySelector('.settings-form');
    if (settingsForm) {
        settingsForm.addEventListener('submit', function() {
            serializeGwarsDomainMap();
        });
    }
    
    
    // Обработка проверки Dadata API
    const verifyDadataBtn = document.getElementById('verify-dadata-btn');
    const verifyStatus = document.getElementById('dadata-verify-status');
    const dadataEnabledCheckbox = document.getElementById('dadata_enabled');
    
    if (verifyDadataBtn) {
        verifyDadataBtn.addEventListener('click', function() {
            const apiKey = document.getElementById('dadata_api_key').value.trim();
            const secretKey = document.getElementById('dadata_secret_key').value.trim();
            
            if (!apiKey || !secretKey) {
                verifyStatus.textContent = '❌ Заполните оба поля';
                verifyStatus.className = 'verify-status error';
                return;
            }
            
            verifyDadataBtn.disabled = true;
            verifyDadataBtn.textContent = '⏳ Проверка...';
            verifyStatus.textContent = '';
            verifyStatus.className = 'verify-status';
            
            const formData = new FormData();
            formData.append('api_key', apiKey);
            formData.append('secret_key', secretKey);
            
            fetch(config.urls.verify_dadata, {
                method: 'POST',
                headers: {
                    'X-CSRF-Token': window.getCsrfToken(),
                },
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    verifyStatus.textContent = '✅ ' + data.message;
                    verifyStatus.className = 'verify-status success';
                    if (dadataEnabledCheckbox) {
                        dadataEnabledCheckbox.disabled = false;
                        // Обновляем подсказку
                        const hint = dadataEnabledCheckbox.closest('.setting-item').querySelector('.setting-hint');
                        if (hint) {
                            hint.textContent = 'Интеграция активна';
                        }
                    }
                } else {
                    verifyStatus.textContent = '❌ ' + data.message;
                    verifyStatus.className = 'verify-status error';
                    if (dadataEnabledCheckbox) {
                        dadataEnabledCheckbox.disabled = true;
                        dadataEnabledCheckbox.checked = false;
                        const hiddenInput = document.getElementById('dadata_enabled_hidden');
                        if (hiddenInput) {
                            hiddenInput.value = '0';
                        }
                        // Обновляем подсказку
                        const hint = dadataEnabledCheckbox.closest('.setting-item').querySelector('.setting-hint');
                        if (hint) {
                            hint.textContent = 'Сначала проверьте ключи';
                        }
                    }
                }
            })
            .catch(error => {
                verifyStatus.textContent = '❌ Ошибка при проверке: ' + error.message;
                verifyStatus.className = 'verify-status error';
            })
            .finally(() => {
                verifyDadataBtn.disabled = false;
                verifyDadataBtn.textContent = '🔍 Проверить ключи';
            });
        });
    }
    
    // Обработка проверки SMTP
    const verifySmtpBtn = document.getElementById('verify-smtp-btn');
    const smtpVerifyStatus = document.getElementById('smtp-verify-status');
    const smtpEnabledCheckbox = document.getElementById('smtp_enabled');
    
    if (verifySmtpBtn) {
        verifySmtpBtn.addEventListener('click', function() {
            const host = document.getElementById('smtp_host').value.trim();
            const port = document.getElementById('smtp_port').value.trim();
            const username = document.getElementById('smtp_username').value.trim();
            const password = document.getElementById('smtp_password').value.trim();
            const useTls = document.getElementById('smtp_use_tls').checked;
            const fromEmail = document.getElementById('smtp_from_email').value.trim();
            
            if (!host || !port || !username || !password || !fromEmail) {
                smtpVerifyStatus.textContent = '❌ Заполните все обязательные поля';
                smtpVerifyStatus.className = 'verify-status error';
                return;
            }
            
            verifySmtpBtn.disabled = true;
            verifySmtpBtn.textContent = '⏳ Проверка...';
            smtpVerifyStatus.textContent = '';
            smtpVerifyStatus.className = 'verify-status';
            
            const formData = new FormData();
            formData.append('host', host);
            formData.append('port', port);
            formData.append('username', username);
            formData.append('password', password);
            formData.append('use_tls', useTls ? '1' : '0');
            formData.append('from_email', fromEmail);
            
            
            fetch(config.urls.verify_smtp, {
                method: 'POST',
                headers: {
                    'X-CSRF-Token': window.getCsrfToken(),
                },
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    smtpVerifyStatus.textContent = '✅ ' + data.message;
                    smtpVerifyStatus.className = 'verify-status success';
                    if (smtpEnabledCheckbox) {
                        smtpEnabledCheckbox.disabled = false;
                        // Обновляем подсказку
                        const hint = smtpEnabledCheckbox.closest('.setting-item').querySelector('.setting-hint');
                        if (hint) {
                            hint.textContent = 'SMTP активен';
                        }
                    }
                } else {
                    smtpVerifyStatus.textContent = '❌ ' + data.message;
                    smtpVerifyStatus.className = 'verify-status error';
                    if (smtpEnabledCheckbox) {
                        smtpEnabledCheckbox.disabled = true;
                        smtpEnabledCheckbox.checked = false;
                        const hiddenInput = document.getElementById('smtp_enabled_hidden');
                        if (hiddenInput) {
                            hiddenInput.value = '0';
                        }
                        // Обновляем подсказку
                        const hint = smtpEnabledCheckbox.closest('.setting-item').querySelector('.setting-hint');
                        if (hint) {
                            hint.textContent = 'Сначала проверьте подключение';
                        }
                    }
                }
            })
            .catch(error => {
                smtpVerifyStatus.textContent = '❌ Ошибка при проверке: ' + error.message;
                smtpVerifyStatus.className = 'verify-status error';
            })
            .finally(() => {
                verifySmtpBtn.disabled = false;
                verifySmtpBtn.textContent = '🔍 Проверить подключение';
            });
        });
    }
    
    // Обработка проверки Telegram бота
    const verifyTelegramBtn = document.getElementById('verify-telegram-btn');
    const telegramVerifyStatus = document.getElementById('telegram-verify-status');
    const telegramEnabledCheckbox = document.getElementById('telegram_enabled');
    const telegramEnabledHidden = document.getElementById('telegram_enabled_hidden');
    
    // Обработчик изменения checkbox для Telegram
    if (telegramEnabledCheckbox && telegramEnabledHidden) {
        telegramEnabledCheckbox.addEventListener('change', function() {
            telegramEnabledHidden.value = this.checked ? '1' : '0';
        });
    }
    
    if (verifyTelegramBtn) {
        verifyTelegramBtn.addEventListener('click', function() {
            const token = document.getElementById('telegram_bot_token').value.trim();
            const chatId = document.getElementById('telegram_chat_id').value.trim();
            
            if (!token) {
                telegramVerifyStatus.textContent = '❌ Токен бота обязателен';
                telegramVerifyStatus.className = 'verify-status error';
                return;
            }
            
            verifyTelegramBtn.disabled = true;
            verifyTelegramBtn.textContent = '⏳ Проверка...';
            telegramVerifyStatus.textContent = '';
            telegramVerifyStatus.className = 'verify-status';
            
            const formData = new FormData();
            formData.append('token', token);
            if (chatId) {
                formData.append('chat_id', chatId);
            }
            
            
            fetch(config.urls.verify_telegram, {
                method: 'POST',
                headers: {
                    'X-CSRF-Token': window.getCsrfToken(),
                },
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    telegramVerifyStatus.textContent = '✅ ' + data.message;
                    telegramVerifyStatus.className = 'verify-status success';
                    if (telegramEnabledCheckbox) {
                        telegramEnabledCheckbox.disabled = false;
                        // Убеждаемся, что hidden input синхронизирован с checkbox
                        if (telegramEnabledHidden) {
                            telegramEnabledHidden.value = telegramEnabledCheckbox.checked ? '1' : '0';
                        }
                        // Обновляем подсказку
                        const hint = telegramEnabledCheckbox.closest('.setting-item').querySelector('.setting-hint');
                        if (hint) {
                            hint.textContent = 'Telegram бот активен';
                        }
                    }
                } else {
                    telegramVerifyStatus.textContent = '❌ ' + data.message;
                    telegramVerifyStatus.className = 'verify-status error';
                    if (telegramEnabledCheckbox) {
                        telegramEnabledCheckbox.disabled = true;
                        telegramEnabledCheckbox.checked = false;
                        const hiddenInput = document.getElementById('telegram_enabled_hidden');
                        if (hiddenInput) {
                            hiddenInput.value = '0';
                        }
                        // Обновляем подсказку
                        const hint = telegramEnabledCheckbox.closest('.setting-item').querySelector('.setting-hint');
                        if (hint) {
                            hint.textContent = 'Сначала проверьте подключение';
                        }
                    }
                }
            })
            .catch(error => {
                telegramVerifyStatus.textContent = '❌ Ошибка при проверке: ' + error.message;
                telegramVerifyStatus.className = 'verify-status error';
            })
            .finally(() => {
                verifyTelegramBtn.disabled = false;
                verifyTelegramBtn.textContent = '🔍 Проверить подключение';
            });
        });
    }
    
    // Синхронизация color picker и text input для цветов
    const colorInputs = [
        { color: 'setting_accent_color', text: 'setting_accent_color_text', preview: 'preview_accent_color' },
        { color: 'setting_accent_color_hover', text: 'setting_accent_color_hover_text', preview: 'preview_accent_color_hover' },
        { color: 'setting_accent_color_dark', text: 'setting_accent_color_dark_text', preview: 'preview_accent_color_dark' },
        { color: 'setting_accent_color_hover_dark', text: 'setting_accent_color_hover_dark_text', preview: 'preview_accent_color_hover_dark' }
    ];
    
    // Функция для обновления превью цвета
    function updateColorPreview(colorValue, previewId) {
        const preview = document.getElementById(previewId);
        if (preview) {
            preview.style.backgroundColor = colorValue;
            const text = preview.querySelector('.color-preview-text');
            if (text) {
                text.textContent = colorValue;
            }
        }
    }
    
    colorInputs.forEach(function(pair) {
        const colorInput = document.getElementById(pair.color);
        const textInput = document.getElementById(pair.text);
        
        if (colorInput && textInput) {
            // При изменении color picker обновляем text input и превью
            colorInput.addEventListener('input', function() {
                const value = colorInput.value;
                textInput.value = value;
                updateColorPreview(value, pair.preview);
            });
            
            // При изменении text input обновляем color picker и превью
            textInput.addEventListener('input', function() {
                const value = textInput.value.trim();
                if (/^#[0-9A-Fa-f]{6}$/.test(value)) {
                    colorInput.value = value;
                    updateColorPreview(value, pair.preview);
                }
            });
        }
    });
    
    // Функция для выделения текущего цвета в палитре
    function highlightCurrentColor(palette, currentColor) {
        palette.querySelectorAll('.color-palette-item').forEach(function(item) {
            const itemColor = item.getAttribute('data-color');
            item.classList.remove('selected');
            if (itemColor && itemColor.toLowerCase() === currentColor.toLowerCase()) {
                item.classList.add('selected');
            }
        });
    }
    
    // Обработка кликов по палитре цветов
    document.querySelectorAll('.color-palette-grid').forEach(function(palette) {
        const targetId = palette.getAttribute('data-target');
        const colorInput = document.getElementById(targetId);
        const textInput = document.getElementById(targetId + '_text');
        const previewId = 'preview_' + targetId;
        
        if (colorInput && textInput) {
            // Выделяем текущий цвет при загрузке страницы
            const currentColor = colorInput.value;
            highlightCurrentColor(palette, currentColor);
            
            // Обработка кликов по цветам в палитре
            palette.querySelectorAll('.color-palette-item').forEach(function(item) {
                item.addEventListener('click', function() {
                    const color = this.getAttribute('data-color');
                    if (color) {
                        colorInput.value = color;
                        textInput.value = color;
                        updateColorPreview(color, previewId);
                        
                        // Визуальная обратная связь - подсветка выбранного цвета
                        highlightCurrentColor(palette, color);
                    }
                });
            });
            
            // Обновляем выделение при изменении через color picker или text input
            colorInput.addEventListener('input', function() {
                highlightCurrentColor(palette, colorInput.value);
            });
            
            textInput.addEventListener('input', function() {
                const value = textInput.value.trim();
                if (/^#[0-9A-Fa-f]{6}$/.test(value)) {
                    highlightCurrentColor(palette, value);
                }
            });
        }
    });
}); // Закрываем document.addEventListener('DOMContentLoaded', function() {

// Управление меню бота


function loadBotMenu() {
    const config = getAdminSettingsConfig();
    const container = document.getElementById('bot-menu-list');
    if (!container) {
        console.log('bot-menu-list container not found');
        return;
    }
    
    console.log('Loading bot menu, items count:', botMenuItems ? botMenuItems.length : 0);
    
    if (botMenuItems.length === 0) {
        container.innerHTML = '<p class="text-muted">Пункты меню не добавлены</p>';
        return;
    }
    
    container.innerHTML = botMenuItems.map(item => {
        const typeLabels = {
            'command': 'Команда',
            'url': 'Ссылка',
            'callback': 'Callback'
        };
        const statusBadge = item.is_active 
            ? '<span class="menu-status-badge menu-status-badge--active">Активен</span>'
            : '<span class="menu-status-badge menu-status-badge--inactive">Неактивен</span>';
        
        return `
            <div class="bot-menu-item">
                <div class="bot-menu-item-info">
                    <div class="bot-menu-item-text">${item.button_text}</div>
                    <div class="bot-menu-item-details">
                        Тип: ${typeLabels[item.button_type] || item.button_type} | 
                        Действие: ${item.action} | 
                        Порядок: ${item.sort_order}
                    </div>
                </div>
                <div class="bot-menu-item-actions">
                    ${statusBadge}
                    <button type="button" class="btn btn-secondary" onclick="editMenuItem(${item.id})" style="padding: 0.5rem 1rem;">✏️</button>
                    <form method="POST" action="${config.urls.telegram_menu}" style="display: inline;" onsubmit="return confirm('Удалить этот пункт меню?');">
        <input type="hidden" name="_csrf_token" value="${config.csrf_token}">
                        <input type="hidden" name="action" value="delete">
                        <input type="hidden" name="menu_id" value="${item.id}">
                        <button type="submit" class="btn btn-secondary" style="padding: 0.5rem 1rem;">🗑️</button>
                    </form>
                </div>
            </div>
        `;
    }).join('');
}

function showMenuModal(action, menuId = null) {
    const modal = document.getElementById('menu-modal');
    const form = document.getElementById('menu-form');
    const title = document.getElementById('menu-modal-title');
    const actionInput = document.getElementById('menu-action');
    const idInput = document.getElementById('menu-id');
    
    if (action === 'create') {
        title.textContent = 'Добавить пункт меню';
        actionInput.value = 'create';
        idInput.value = '';
        form.reset();
        document.getElementById('menu-sort-order').value = '100';
        document.getElementById('menu-is-active').checked = true;
    } else if (action === 'edit' && menuId) {
        title.textContent = 'Редактировать пункт меню';
        actionInput.value = 'update';
        idInput.value = menuId;
        
        const item = botMenuItems.find(m => m.id === menuId);
        if (item) {
            document.getElementById('menu-button-text').value = item.button_text;
            document.getElementById('menu-button-type').value = item.button_type;
            document.getElementById('menu-action-value').value = item.action;
            document.getElementById('menu-sort-order').value = item.sort_order;
            document.getElementById('menu-is-active').checked = item.is_active == 1;
            updateMenuActionPlaceholder();
        }
    }
    
    modal.classList.add('active');
}

function closeMenuModal() {
    document.getElementById('menu-modal').classList.remove('active');
}

function editMenuItem(menuId) {
    showMenuModal('edit', menuId);
}

function updateMenuActionPlaceholder() {
    const type = document.getElementById('menu-button-type').value;
    const actionInput = document.getElementById('menu-action-value');
    const hint = document.getElementById('menu-action-hint');
    
    if (type === 'command') {
        actionInput.placeholder = 'events';
        hint.innerHTML = 'Для команды: название команды (например: events, assignments)';
    } else if (type === 'url') {
        actionInput.placeholder = '/faq';
        hint.innerHTML = 'Для ссылки: URL (например: /faq, /rules или полный URL)';
    } else if (type === 'callback') {
        actionInput.placeholder = 'callback_data';
        hint.innerHTML = 'Для callback: callback_data';
    }
}

function copyWebhookUrl() {
    const url = document.getElementById('webhook-url').textContent;
    navigator.clipboard.writeText(url).then(() => {
        alert('URL вебхука скопирован в буфер обмена!');
    }).catch(() => {
        // Fallback для старых браузеров
        const textarea = document.createElement('textarea');
        textarea.value = url;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
        alert('URL вебхука скопирован в буфер обмена!');
    });
}

// Загружаем меню при загрузке страницы и при переключении на вкладку Telegram
document.addEventListener('DOMContentLoaded', function() {
    const config = getAdminSettingsConfig();
    botMenuItems = config.bot_menu_items || [];
    // Загружаем меню сразу
    loadBotMenu();
    
    // Обработка переключения подтабов интеграций (для загрузки меню бота)
    const integrationSubtabsForMenu = document.querySelectorAll('.integration-subtab');
    if (integrationSubtabsForMenu.length > 0) {
        integrationSubtabsForMenu.forEach(subtab => {
            subtab.addEventListener('click', function() {
                const integration = this.dataset.integration;
                
                // Если переключились на Telegram, загружаем меню
                if (integration === 'telegram') {
                    setTimeout(() => {
                        loadBotMenu();
                    }, 100);
                }
            });
        });
    }
    
    // Обновляем меню после сохранения (если страница не перезагрузилась)
    const menuForm = document.getElementById('menu-form');
    if (menuForm) {
        menuForm.addEventListener('submit', function(e) {
            // Даем время на сохранение, затем перезагружаем страницу
            setTimeout(() => {
                location.reload();
            }, 500);
        });
    }
});
