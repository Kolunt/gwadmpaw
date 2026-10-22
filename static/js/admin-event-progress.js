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
    var notesListEl = document.getElementById('progress-workspace-notes-list');
    var tasksListEl = document.getElementById('progress-workspace-tasks-list');
    var noteForm = document.getElementById('progress-workspace-note-form');
    var noteInput = document.getElementById('progress-workspace-note-input');
    var taskForm = document.getElementById('progress-workspace-task-form');
    var taskInput = document.getElementById('progress-workspace-task-input');
    var taskDueInput = document.getElementById('progress-workspace-task-due');

    var currentAssignmentId = null;

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
        var s = String(dueAt).replace('T', ' ').slice(0, 16);
        return s;
    }

    function dueAtToLocalInput(dueAt) {
        if (!dueAt) return '';
        var s = String(dueAt).replace(' ', 'T').slice(0, 16);
        return s;
    }

    function isTaskOverdue(task) {
        if (task.is_completed || !task.due_at) return false;
        var due = new Date(String(task.due_at).replace(' ', 'T'));
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

    function renderNotes(notes) {
        if (!notesListEl) return;
        if (!notes.length) {
            notesListEl.innerHTML = '<p class="text-muted progress-workspace-empty">Заметок пока нет</p>';
            return;
        }
        notesListEl.innerHTML = notes.map(function (note) {
            return (
                '<div class="progress-workspace-item" data-note-id="' + note.id + '">' +
                '<div class="progress-workspace-item-view">' +
                '<p class="progress-workspace-item-body">' + esc(note.body) + '</p>' +
                '<div class="progress-workspace-item-meta text-muted">' + esc(note.updated_at || note.created_at || '') + '</div>' +
                '<div class="progress-workspace-item-actions">' +
                '<button type="button" class="btn btn-outline-secondary btn-sm js-note-edit">Изменить</button>' +
                '<button type="button" class="btn btn-outline-danger btn-sm js-note-delete">Удалить</button>' +
                '</div></div>' +
                '<div class="progress-workspace-item-edit" style="display:none;">' +
                '<textarea class="setting-input js-note-edit-input" rows="2">' + esc(note.body) + '</textarea>' +
                '<button type="button" class="btn btn-primary btn-sm js-note-save">Сохранить</button>' +
                '<button type="button" class="btn btn-secondary btn-sm js-note-cancel">Отмена</button>' +
                '</div></div>'
            );
        }).join('');
    }

    function renderTasks(tasks) {
        if (!tasksListEl) return;
        if (!tasks.length) {
            tasksListEl.innerHTML = '<p class="text-muted progress-workspace-empty">Задач пока нет</p>';
            return;
        }
        tasksListEl.innerHTML = tasks.map(function (task) {
            var overdue = isTaskOverdue(task);
            var rowClass = 'progress-workspace-item progress-workspace-task-item';
            if (task.is_completed) rowClass += ' progress-workspace-task-done';
            if (overdue) rowClass += ' progress-task-overdue';
            var dueLine = task.due_at
                ? '<span class="progress-workspace-task-due">Срок: ' + esc(formatDueDisplay(task.due_at)) + '</span>'
                : '';
            return (
                '<div class="' + rowClass + '" data-task-id="' + task.id + '">' +
                '<div class="progress-workspace-item-view">' +
                '<label class="progress-workspace-task-check">' +
                '<input type="checkbox" class="js-task-complete" ' + (task.is_completed ? 'checked' : '') + '> ' +
                '<span class="progress-workspace-item-body">' + esc(task.body) + '</span></label>' +
                dueLine +
                '<div class="progress-workspace-item-actions">' +
                '<button type="button" class="btn btn-outline-secondary btn-sm js-task-edit">Изменить</button>' +
                '<button type="button" class="btn btn-outline-danger btn-sm js-task-delete">Удалить</button>' +
                '</div></div>' +
                '<div class="progress-workspace-item-edit" style="display:none;">' +
                '<input type="text" class="setting-input js-task-edit-body" value="' + esc(task.body) + '">' +
                '<label class="progress-workspace-due-label"><span>Срок</span>' +
                '<input type="datetime-local" class="setting-input js-task-edit-due" value="' + esc(dueAtToLocalInput(task.due_at)) + '"></label>' +
                '<button type="button" class="btn btn-primary btn-sm js-task-save">Сохранить</button>' +
                '<button type="button" class="btn btn-secondary btn-sm js-task-cancel">Отмена</button>' +
                '</div></div>'
            );
        }).join('');
    }

    function applyWorkspaceData(data) {
        renderNotes(data.notes || []);
        renderTasks(data.tasks || []);
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
        if (noteInput) noteInput.value = '';
        if (taskInput) taskInput.value = '';
        if (taskDueInput) taskDueInput.value = '';
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
            var aid = wsBtn.getAttribute('data-assignment-id');
            openWorkspace(aid, wsBtn.getAttribute('data-pair-label'));
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

    if (noteForm) {
        noteForm.addEventListener('submit', function (e) {
            e.preventDefault();
            if (!currentAssignmentId) return;
            hideWorkspaceError();
            apiFetch(workspaceBase(currentAssignmentId) + '/notes', {
                method: 'POST',
                body: { body: noteInput.value },
            }).then(function (data) {
                applyWorkspaceData(data);
                noteInput.value = '';
            }).catch(function (err) {
                showWorkspaceError(err.message);
            });
        });
    }

    if (taskForm) {
        taskForm.addEventListener('submit', function (e) {
            e.preventDefault();
            if (!currentAssignmentId) return;
            hideWorkspaceError();
            var due = taskDueInput.value || null;
            apiFetch(workspaceBase(currentAssignmentId) + '/tasks', {
                method: 'POST',
                body: { body: taskInput.value, due_at: due },
            }).then(function (data) {
                applyWorkspaceData(data);
                taskInput.value = '';
                taskDueInput.value = '';
            }).catch(function (err) {
                showWorkspaceError(err.message);
            });
        });
    }

    function toggleEdit(item, showEdit) {
        var view = item.querySelector('.progress-workspace-item-view');
        var edit = item.querySelector('.progress-workspace-item-edit');
        if (view) view.style.display = showEdit ? 'none' : '';
        if (edit) edit.style.display = showEdit ? '' : 'none';
    }

    if (notesListEl) {
        notesListEl.addEventListener('click', function (e) {
            var item = e.target.closest('[data-note-id]');
            if (!item || !currentAssignmentId) return;
            var noteId = item.getAttribute('data-note-id');
            if (e.target.closest('.js-note-edit')) {
                toggleEdit(item, true);
                return;
            }
            if (e.target.closest('.js-note-cancel')) {
                toggleEdit(item, false);
                return;
            }
            if (e.target.closest('.js-note-delete')) {
                if (!window.confirm('Удалить заметку?')) return;
                apiFetch(workspaceBase(currentAssignmentId) + '/notes/' + noteId, { method: 'DELETE' })
                    .then(applyWorkspaceData)
                    .catch(function (err) { showWorkspaceError(err.message); });
                return;
            }
            if (e.target.closest('.js-note-save')) {
                var text = item.querySelector('.js-note-edit-input').value;
                apiFetch(workspaceBase(currentAssignmentId) + '/notes/' + noteId, {
                    method: 'PATCH',
                    body: { body: text },
                }).then(applyWorkspaceData).catch(function (err) { showWorkspaceError(err.message); });
            }
        });
    }

    if (tasksListEl) {
        tasksListEl.addEventListener('click', function (e) {
            var item = e.target.closest('[data-task-id]');
            if (!item || !currentAssignmentId) return;
            var taskId = item.getAttribute('data-task-id');
            if (e.target.classList.contains('js-task-complete')) {
                apiFetch(workspaceBase(currentAssignmentId) + '/tasks/' + taskId, {
                    method: 'PATCH',
                    body: { completed: e.target.checked },
                }).then(applyWorkspaceData).catch(function (err) {
                    showWorkspaceError(err.message);
                    e.target.checked = !e.target.checked;
                });
                return;
            }
            if (e.target.closest('.js-task-edit')) {
                toggleEdit(item, true);
                return;
            }
            if (e.target.closest('.js-task-cancel')) {
                toggleEdit(item, false);
                return;
            }
            if (e.target.closest('.js-task-delete')) {
                if (!window.confirm('Удалить задачу?')) return;
                apiFetch(workspaceBase(currentAssignmentId) + '/tasks/' + taskId, { method: 'DELETE' })
                    .then(applyWorkspaceData)
                    .catch(function (err) { showWorkspaceError(err.message); });
                return;
            }
            if (e.target.closest('.js-task-save')) {
                var body = item.querySelector('.js-task-edit-body').value;
                var due = item.querySelector('.js-task-edit-due').value;
                apiFetch(workspaceBase(currentAssignmentId) + '/tasks/' + taskId, {
                    method: 'PATCH',
                    body: { body: body, due_at: due || null },
                }).then(applyWorkspaceData).catch(function (err) { showWorkspaceError(err.message); });
            }
        });
    }
})();
