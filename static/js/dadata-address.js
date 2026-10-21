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
        function debounced() {
            var args = arguments;
            var self = this;
            clearTimeout(timer);
            timer = setTimeout(function () {
                fn.apply(self, args);
            }, ms);
        }
        debounced.cancel = function () {
            clearTimeout(timer);
        };
        return debounced;
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
        var mode = options.mode || 'address';
        var getExtraPayload = options.getExtraPayload;
        var onSelect = options.onSelect;
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
        var suppressFetch = false;

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
            fetchSuggestions.cancel();
            suppressFetch = true;
            fillFields(resolvedFields, item.fields || {});
            if (mode === 'street') {
                if (item.fields && item.fields.street) {
                    searchInput.value = item.fields.street;
                }
            } else if (item.value) {
                searchInput.value = item.value;
            }
            if (typeof onSelect === 'function') {
                onSelect(item);
            }
            closeList();
            fetchSuggestions.cancel();
            suppressFetch = false;
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
            if (suppressFetch) {
                return;
            }
            var query = (searchInput.value || '').trim();
            if (query.length < 2) {
                closeList();
                return;
            }
            var extra = typeof getExtraPayload === 'function' ? getExtraPayload() : {};
            if (extra && extra.skip) {
                closeList();
                return;
            }
            var payload = { query: query };
            if (extra && typeof extra === 'object') {
                Object.keys(extra).forEach(function (key) {
                    if (key !== 'skip') {
                        payload[key] = extra[key];
                    }
                });
            }
            fetch(suggestUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'X-CSRF-Token': getCsrf(),
                },
                credentials: 'same-origin',
                body: JSON.stringify(payload),
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

        searchInput._dadataCloseList = closeList;
    };

    var RUSSIA_COUNTRY_VALUES = { 'россия': true, russia: true, 'рф': true, ru: true };

    window.isRussiaCountry = function (country) {
        var normalized = (country || '').trim().toLowerCase();
        if (!normalized) {
            return true;
        }
        return Object.prototype.hasOwnProperty.call(RUSSIA_COUNTRY_VALUES, normalized);
    };

    window.bindAddressRegionToggle = function (options) {
        var radiogroup = options.radiogroup;
        var dadataWrap = options.dadataWrap;
        var countryInput = options.countryInput;
        var searchInput = options.searchInput;
        var streetSearchInput = options.streetSearchInput;
        var extraHides = options.extraHides || [];
        if (!radiogroup || !dadataWrap) {
            return { syncFromCountry: function () {} };
        }

        var radios = radiogroup.querySelectorAll('input[type="radio"]');

        function closeSuggestions() {
            [searchInput, streetSearchInput].forEach(function (input) {
                if (input && typeof input._dadataCloseList === 'function') {
                    input._dadataCloseList();
                }
            });
        }

        function apply(region) {
            var showDadata = region === 'ru';
            dadataWrap.hidden = !showDadata;
            extraHides.forEach(function (el) {
                if (el) {
                    el.hidden = !showDadata;
                }
            });
            if (!showDadata) {
                closeSuggestions();
            }
        }

        function syncFromCountry() {
            var fromRussia = countryInput
                ? window.isRussiaCountry(countryInput.value)
                : true;
            var region = fromRussia ? 'ru' : 'other';
            radios.forEach(function (radio) {
                radio.checked = radio.value === region;
            });
            apply(region);
        }

        radios.forEach(function (radio) {
            radio.addEventListener('change', function () {
                if (radio.checked) {
                    apply(radio.value);
                }
            });
        });

        if (countryInput) {
            countryInput.addEventListener('input', syncFromCountry);
            countryInput.addEventListener('change', syncFromCountry);
        }

        syncFromCountry();

        return { syncFromCountry: syncFromCountry };
    };

    window.buildDadataLocationsPayload = function (storedLocation, fallbackFields) {
        var locations = [];
        if (storedLocation && typeof storedLocation === 'object') {
            var copy = {};
            Object.keys(storedLocation).forEach(function (key) {
                if (storedLocation[key]) {
                    copy[key] = storedLocation[key];
                }
            });
            if (Object.keys(copy).length) {
                locations.push(copy);
            }
        }
        if (!locations.length && fallbackFields) {
            var fallback = {};
            if (fallbackFields.postal_code && fallbackFields.postal_code.value) {
                fallback.postal_code = fallbackFields.postal_code.value.trim();
            }
            if (fallbackFields.city && fallbackFields.city.value) {
                fallback.city = fallbackFields.city.value.trim();
            }
            if (Object.keys(fallback).length) {
                locations.push(fallback);
            }
        }
        return locations.length ? locations : null;
    };
})();
