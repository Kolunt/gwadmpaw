(function () {
    var board = document.querySelector('.admin-event-progress-page');
    if (!board) {
        return;
    }
    var eventId = board.getAttribute('data-event-id');

    var contactModal = document.getElementById('progress-contact-modal');
    var contactBodyEl = document.getElementById('progress-contact-modal-body');
    var contactTitleEl = document.getElementById('progress-contact-modal-title');
    var contactProfileLink = document.getElementById('progress-contact-profile-link');
    var contactCloseBtn = document.getElementById('progress-contact-modal-close');

    var workspaceModal = document.getElementById('progress-workspace-modal');
    var workspaceCloseBtn = document.getElementById('progress-workspace-modal-close');
    var workspaceTitleEl = document.getElementById('progress-workspace-modal-title');
    var workspacePairLabel = document.getElementById('progress-workspace-pair-label');
    var workspaceErrorEl = document.getElementById('progress-workspace-error');
    var entriesListEl = document.getElementById('progress-workspace-entries-list');
    var entryForm = document.getElementById('progress-workspace-entry-form');
    var entryInput = document.getElementById('progress-workspace-entry-input');
    var entryDueInput = document.getElementById('progress-workspace-entry-due');
    var entryDuePickerBtn = entryDueInput
        ? entryDueInput.closest('.datetime-field-wrap')?.querySelector('.datetime-picker-btn')
        : null;

    var currentAssignmentId = null;

    if (entryDueInput && entryDuePickerBtn) {
        entryDuePickerBtn.addEventListener('click', function () {
            if (typeof entryDueInput.showPicker === 'function') {
                try {
                    entryDueInput.showPicker();
                    return;
                } catch (e) {
                    /* showPicker may throw without user gesture or in unsupported builds */
                }
            }
            entryDueInput.focus();
            entryDueInput.click();
        });
    }

    function esc(text) {
        var div = document.createElement('div');
        div.textContent = text == null ? '' : String(text);
        return div.innerHTML;
    }

    function tgHandle(raw) {
        var t = (raw || '').trim();
        if (!t) return '';
        return t.charAt(0) === '@' ? t : '@' + t;
    }

    function tgUrl(raw) {
        var t = (raw || '').trim().replace(/^@/, '');
        return t ? 'https://t.me/' + encodeURIComponent(t) : '';
    }

    function digitsOnly(s) {
        return (s || '').replace(/\s/g, '');
    }

    function openContacts(data) {
        if (!data || !contactModal) return;
        contactTitleEl.textContent = 'Контакты: ' + (data.username || '—');
        var html = '';
        if (data.email) {
            html += '<div><strong>Email:</strong> <a href="mailto:' + esc(data.email) + '" class="contact-link">' + esc(data.email) + '</a></div>';
        }
        if (data.phone) {
            var tel = digitsOnly(data.phone);
            html += '<div><strong>Телефон:</strong> <a href="tel:' + esc(tel) + '" class="contact-link">' + esc(data.phone) + '</a></div>';
        }
        if (data.telegram) {
            html += '<div><strong>Telegram:</strong> <a href="' + esc(tgUrl(data.telegram)) + '" class="contact-link" target="_blank" rel="noopener">' + esc(tgHandle(data.telegram)) + '</a></div>';
        }
        if (data.whatsapp) {
            var wa = digitsOnly(data.whatsapp);
            html += '<div><strong>WhatsApp:</strong> <a href="https://wa.me/' + esc(wa) + '" class="contact-link" target="_blank" rel="noopener">' + esc(data.whatsapp) + '</a></div>';
        }
        if (data.viber) {
            var vb = digitsOnly(data.viber);
            html += '<div><strong>Viber:</strong> <a href="viber://chat?number=' + esc(vb) + '" class="contact-link">' + esc(data.viber) + '</a></div>';
        }
        if (data.max) {
            var mx = digitsOnly(data.max);
            html += '<div><strong>Max:</strong> <a href="tel:' + esc(mx) + '" class="contact-link">' + esc(data.max) + '</a></div>';
        }
        if (!html) {
            html = '<span class="text-muted">Контакты не указаны</span>';
        }
        contactBodyEl.innerHTML = html;
        if (data.user_id) {
            contactProfileLink.href = '/profile/' + data.user_id;
            contactProfileLink.style.display = '';
        } else {
            contactProfileLink.style.display = 'none';
        }
        contactModal.style.display = 'block';
    }

    function closeContactModal() {
        if (contactModal) contactModal.style.display = 'none';
    }

    function closeWorkspaceModal() {
        if (workspaceModal) workspaceModal.style.display = 'none';
        currentAssignmentId = null;
        hideWorkspaceError();
    }

    function hideWorkspaceError() {
        if (workspaceErrorEl) {
            workspaceErrorEl.style.display = 'none';
            workspaceErrorEl.textContent = '';
        }
    }

    function showWorkspaceError(msg) {
        if (workspaceErrorEl) {
            workspaceErrorEl.textContent = msg;
            workspaceErrorEl.style.display = 'block';
        }
    }

    function workspaceBase(assignmentId) {
        return '/admin/events/' + encodeURIComponent(eventId) + '/assignments/' + encodeURIComponent(assignmentId) + '/workspace';
    }

    function apiFetch(url, options) {
        options = options || {};
        options.headers = options.headers || {};
        options.headers['X-CSRF-Token'] = window.getCsrfToken();
        if (options.body && typeof options.body === 'object') {
            options.headers['Content-Type'] = 'application/json';
            options.body = JSON.stringify(options.body);
        }
        options.headers['Accept'] = 'application/json';
        options.headers['X-Requested-With'] = 'XMLHttpRequest';
        return fetch(url, options).then(function (resp) {
            return resp.json().then(function (data) {
                if (!resp.ok) {
                    var err = new Error(data.error || 'Ошибка запроса');
                    err.status = resp.status;
                    throw err;
                }
                return data;
            });
        });
    }

    function formatDueDisplay(dueAt) {
        if (!dueAt) return '';
        return String(dueAt).replace('T', ' ').slice(0, 16);
    }

    function formatMeta(entry) {
        var author = entry.author_username || '—';
        var when = entry.created_at ? String(entry.created_at).replace('T', ' ').slice(0, 19) : '';
        return 'Автор: ' + author + (when ? ' · ' + when : '');
    }

    function isEntryOverdue(entry) {
        if (entry.kind !== 'task' || !entry.show_checkbox || entry.is_completed || !entry.due_at) {
            return false;
        }
        var due = new Date(String(entry.due_at).replace(' ', 'T'));
        return due.getTime() < Date.now();
    }

    function syncCardPulse(assignmentId, openCount) {
        var card = board.querySelector('.progress-kanban-card[data-assignment-id="' + assignmentId + '"]');
        if (!card) return;
        card.setAttribute('data-open-tasks', String(openCount));
        if (openCount > 0) {
            card.classList.add('progress-kanban-card-pulse');
        } else {
            card.classList.remove('progress-kanban-card-pulse');
        }
        var btn = card.querySelector('.js-progress-workspace');
        if (btn) {
            var base = 'Работа';
            btn.textContent = openCount > 0 ? base + ' (' + openCount + ')' : base;
        }
    }

    function renderEntries(entries) {
        if (!entriesListEl) return;
        if (!entries || !entries.length) {
            entriesListEl.innerHTML = '<p class="text-muted progress-workspace-empty">Записей пока нет</p>';
            return;
        }
        entriesListEl.innerHTML = entries.map(function (entry) {
            var rowClass = 'progress-workspace-item';
            if (entry.kind === 'task') {
                rowClass += ' progress-workspace-task-item';
                if (entry.is_completed) rowClass += ' progress-workspace-task-done';
                if (isEntryOverdue(entry)) rowClass += ' progress-task-overdue';
            }
            var bodyHtml;
            if (entry.show_checkbox) {
                bodyHtml =
                    '<label class="progress-workspace-task-check">' +
                    '<input type="checkbox" class="js-task-complete" data-task-id="' + entry.id + '" ' +
                    (entry.is_completed ? 'checked' : '') + '> ' +
                    '<span class="progress-workspace-item-body">' + esc(entry.body) + '</span></label>';
                if (entry.due_at) {
                    bodyHtml += '<span class="progress-workspace-task-due">Срок: ' + esc(formatDueDisplay(entry.due_at)) + '</span>';
                }
            } else {
                bodyHtml = '<p class="progress-workspace-item-body">' + esc(entry.body) + '</p>';
            }
            return (
                '<div class="' + rowClass + '" data-entry-kind="' + esc(entry.kind) + '" data-entry-id="' + entry.id + '">' +
                bodyHtml +
                '<div class="progress-workspace-item-meta text-muted">' + esc(formatMeta(entry)) + '</div>' +
                '</div>'
            );
        }).join('');
    }

    function applyWorkspaceData(data) {
        renderEntries(data.entries || []);
        syncCardPulse(data.assignment_id, data.open_tasks_count || 0);
    }

    function loadWorkspace(assignmentId) {
        return apiFetch(workspaceBase(assignmentId)).then(function (data) {
            applyWorkspaceData(data);
            return data;
        });
    }

    function openWorkspace(assignmentId, pairLabel) {
        currentAssignmentId = assignmentId;
        hideWorkspaceError();
        workspaceTitleEl.textContent = 'Работа с парой';
        workspacePairLabel.textContent = pairLabel || '—';
        if (entryInput) entryInput.value = '';
        if (entryDueInput) entryDueInput.value = '';
        workspaceModal.style.display = 'block';
        loadWorkspace(assignmentId).catch(function (err) {
            showWorkspaceError(err.message || 'Не удалось загрузить данные');
        });
    }

    board.addEventListener('click', function (e) {
        var contactBtn = e.target.closest('.js-progress-contact');
        if (contactBtn) {
            var raw = contactBtn.getAttribute('data-contact');
            if (!raw) return;
            try {
                openContacts(JSON.parse(raw));
            } catch (err) { /* ignore */ }
            return;
        }
        var wsBtn = e.target.closest('.js-progress-workspace');
        if (wsBtn) {
            openWorkspace(wsBtn.getAttribute('data-assignment-id'), wsBtn.getAttribute('data-pair-label'));
        }
    });

    if (contactCloseBtn) contactCloseBtn.addEventListener('click', closeContactModal);
    if (contactModal) {
        contactModal.addEventListener('click', function (e) {
            if (e.target === contactModal) closeContactModal();
        });
    }
    if (workspaceCloseBtn) workspaceCloseBtn.addEventListener('click', closeWorkspaceModal);
    if (workspaceModal) {
        workspaceModal.addEventListener('click', function (e) {
            if (e.target === workspaceModal) closeWorkspaceModal();
        });
    }
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            closeContactModal();
            closeWorkspaceModal();
        }
    });

    if (entryForm) {
        entryForm.addEventListener('submit', function (e) {
            e.preventDefault();
            if (!currentAssignmentId) return;
            hideWorkspaceError();
            var text = (entryInput.value || '').trim();
            if (!text) {
                showWorkspaceError('Введите текст');
                return;
            }
            var due = entryDueInput && entryDueInput.value ? entryDueInput.value : null;
            var url = workspaceBase(currentAssignmentId) + (due ? '/tasks' : '/notes');
            var body = due ? { body: text, due_at: due } : { body: text };
            apiFetch(url, { method: 'POST', body: body }).then(function (data) {
                applyWorkspaceData(data);
                entryInput.value = '';
                if (entryDueInput) entryDueInput.value = '';
            }).catch(function (err) {
                showWorkspaceError(err.message);
            });
        });
    }

    if (entriesListEl) {
        entriesListEl.addEventListener('change', function (e) {
            if (!e.target.classList.contains('js-task-complete') || !currentAssignmentId) {
                return;
            }
            var taskId = e.target.getAttribute('data-task-id');
            apiFetch(workspaceBase(currentAssignmentId) + '/tasks/' + taskId, {
                method: 'PATCH',
                body: { completed: e.target.checked },
            }).then(applyWorkspaceData).catch(function (err) {
                showWorkspaceError(err.message);
                e.target.checked = !e.target.checked;
            });
        });
    }
})();
