(function () {
    function getCsrf() {
        if (typeof window.getCsrfToken === 'function') {
            return window.getCsrfToken();
        }
        var meta = document.querySelector('meta[name="csrf-token"]');
        return meta ? meta.getAttribute('content') : '';
    }

    function debounce(fn, ms) {
        var timer;
        return function () {
            var args = arguments;
            var self = this;
            clearTimeout(timer);
            timer = setTimeout(function () {
                fn.apply(self, args);
            }, ms);
        };
    }

    function fillFields(fieldsMap, data) {
        if (!data || !fieldsMap) {
            return;
        }
        Object.keys(fieldsMap).forEach(function (key) {
            var el = fieldsMap[key];
            if (!el || data[key] === undefined) {
                return;
            }
            el.value = data[key] || '';
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        });
    }

    window.initDadataAddressSearch = function (options) {
        var searchInput = options.searchInput;
        var suggestionsRoot = options.suggestionsRoot;
        var fieldsMap = options.fields || {};
        var suggestUrl = options.suggestUrl;
        if (!searchInput || !suggestionsRoot || !suggestUrl) {
            return;
        }

        var resolvedFields = {};
        Object.keys(fieldsMap).forEach(function (key) {
            var target = fieldsMap[key];
            if (typeof target === 'string') {
                resolvedFields[key] = document.querySelector(target);
            } else {
                resolvedFields[key] = target;
            }
        });

        var listEl = document.createElement('div');
        listEl.className = 'dadata-suggestions';
        listEl.setAttribute('role', 'listbox');
        listEl.hidden = true;
        suggestionsRoot.appendChild(listEl);

        var activeIndex = -1;
        var currentItems = [];

        function closeList() {
            listEl.hidden = true;
            listEl.innerHTML = '';
            activeIndex = -1;
            currentItems = [];
            searchInput.setAttribute('aria-expanded', 'false');
        }

        function openList() {
            listEl.hidden = false;
            searchInput.setAttribute('aria-expanded', 'true');
        }

        function selectItem(index) {
            if (index < 0 || index >= currentItems.length) {
                return;
            }
            var item = currentItems[index];
            fillFields(resolvedFields, item.fields || {});
            if (item.value) {
                searchInput.value = item.value;
            }
            closeList();
        }

        function renderList(items) {
            listEl.innerHTML = '';
            currentItems = items;
            activeIndex = -1;
            if (!items.length) {
                closeList();
                return;
            }
            items.forEach(function (item, idx) {
                var btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'dadata-suggestion-item';
                btn.setAttribute('role', 'option');
                btn.textContent = item.label || item.value || '';
                btn.addEventListener('click', function () {
                    selectItem(idx);
                });
                listEl.appendChild(btn);
            });
            openList();
        }

        var fetchSuggestions = debounce(function () {
            var query = (searchInput.value || '').trim();
            if (query.length < 2) {
                closeList();
                return;
            }
            fetch(suggestUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'X-CSRF-Token': getCsrf(),
                },
                credentials: 'same-origin',
                body: JSON.stringify({ query: query }),
            })
                .then(function (response) {
                    return response.json().then(function (body) {
                        return { ok: response.ok, body: body };
                    });
                })
                .then(function (result) {
                    if (!result.ok || !result.body || !result.body.success) {
                        closeList();
                        return;
                    }
                    renderList(result.body.suggestions || []);
                })
                .catch(function () {
                    closeList();
                });
        }, 350);

        searchInput.setAttribute('aria-autocomplete', 'list');
        searchInput.setAttribute('aria-expanded', 'false');
        searchInput.addEventListener('input', fetchSuggestions);
        searchInput.addEventListener('keydown', function (event) {
            if (event.key === 'Escape') {
                closeList();
                return;
            }
            if (listEl.hidden || !currentItems.length) {
                return;
            }
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                activeIndex = Math.min(activeIndex + 1, currentItems.length - 1);
            } else if (event.key === 'ArrowUp') {
                event.preventDefault();
                activeIndex = Math.max(activeIndex - 1, 0);
            } else if (event.key === 'Enter' && activeIndex >= 0) {
                event.preventDefault();
                selectItem(activeIndex);
                return;
            } else {
                return;
            }
            var buttons = listEl.querySelectorAll('.dadata-suggestion-item');
            buttons.forEach(function (btn, idx) {
                btn.classList.toggle('active', idx === activeIndex);
            });
        });

        document.addEventListener('click', function (event) {
            if (!suggestionsRoot.contains(event.target) && event.target !== searchInput) {
                closeList();
            }
        });
    };
})();
