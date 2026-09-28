// Управление сайдбаром
(function () {
    'use strict';

    const SIDEBAR_COLLAPSED_KEY = 'gwadm-sidebar-collapsed';
    const MOBILE_MAX = 1024;
    let initialized = false;
    let resizeTimer = null;

    function isMobile() {
        return window.innerWidth < MOBILE_MAX;
    }

    function isCollapsed() {
        return document.documentElement.classList.contains('sidebar-collapsed');
    }

    function getLinkLabel(link) {
        const label = link.querySelector('span:not(.sidebar-icon)');
        return label ? label.textContent.trim() : link.textContent.trim();
    }

    function updateCollapseButton(collapsed) {
        const sidebarCollapse = document.getElementById('sidebar-collapse');
        if (!sidebarCollapse) {
            return;
        }
        sidebarCollapse.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        sidebarCollapse.setAttribute(
            'aria-label',
            collapsed ? 'Развернуть меню' : 'Свернуть меню'
        );
    }

    function updateLinkTitles(collapsed) {
        document.querySelectorAll('.sidebar-link').forEach(function (link) {
            const label = getLinkLabel(link);
            if (collapsed && label) {
                link.setAttribute('title', label);
            } else {
                link.removeAttribute('title');
            }
        });
    }

    function setCollapsed(collapsed, persist) {
        if (isMobile()) {
            document.documentElement.classList.remove('sidebar-collapsed');
            updateLinkTitles(false);
            updateCollapseButton(false);
            return;
        }

        document.documentElement.classList.toggle('sidebar-collapsed', collapsed);
        updateCollapseButton(collapsed);
        updateLinkTitles(collapsed);

        if (persist !== false) {
            try {
                localStorage.setItem(SIDEBAR_COLLAPSED_KEY, collapsed ? '1' : '0');
            } catch (e) { /* ignore */ }
        }
    }

    function toggleCollapsed() {
        setCollapsed(!isCollapsed());
    }

    function applyStoredCollapseState() {
        if (isMobile()) {
            setCollapsed(false, false);
            return;
        }

        setCollapsed(getStoredCollapsed(), false);
    }

    function getStoredCollapsed() {
        try {
            return localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1';
        } catch (e) {
            return false;
        }
    }

    function initSidebar() {
        const sidebar = document.getElementById('sidebar');
        const sidebarToggle = document.getElementById('sidebar-toggle');
        const sidebarClose = document.getElementById('sidebar-close');
        const sidebarOverlay = document.getElementById('sidebar-overlay');
        const sidebarCollapse = document.getElementById('sidebar-collapse');

        if (!sidebar || !sidebarToggle) {
            return;
        }

        if (initialized) {
            applyStoredCollapseState();
            return;
        }
        initialized = true;

        function openSidebar() {
            sidebar.classList.add('active');
            if (sidebarOverlay) {
                sidebarOverlay.classList.add('active');
            }
            sidebarToggle.classList.add('active');
            document.body.style.overflow = 'hidden';
        }

        function closeSidebar() {
            sidebar.classList.remove('active');
            if (sidebarOverlay) {
                sidebarOverlay.classList.remove('active');
            }
            sidebarToggle.classList.remove('active');
            document.body.style.overflow = '';
        }

        sidebarToggle.addEventListener('click', function (e) {
            e.stopPropagation();
            if (isMobile()) {
                if (sidebar.classList.contains('active')) {
                    closeSidebar();
                } else {
                    openSidebar();
                }
                return;
            }
            toggleCollapsed();
        });

        if (sidebarCollapse) {
            sidebarCollapse.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                if (!isMobile()) {
                    toggleCollapsed();
                }
            });
        }

        if (sidebarClose) {
            sidebarClose.addEventListener('click', function (e) {
                e.stopPropagation();
                closeSidebar();
            });
        }

        if (sidebarOverlay) {
            sidebarOverlay.addEventListener('click', function (e) {
                e.stopPropagation();
                closeSidebar();
            });
        }

        document.querySelectorAll('.sidebar-link').forEach(function (link) {
            link.addEventListener('click', function () {
                if (isMobile()) {
                    setTimeout(closeSidebar, 100);
                }
            });
        });

        window.addEventListener('resize', function () {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(function () {
                if (!isMobile()) {
                    closeSidebar();
                    applyStoredCollapseState();
                    return;
                }
                setCollapsed(false, false);
            }, 120);
        });

        applyStoredCollapseState();

        if (isMobile()) {
            closeSidebar();
        }

        let touchStartX = 0;
        let touchStartY = 0;
        const minSwipeDistance = 50;
        const maxVerticalSwipe = 30;

        document.addEventListener('touchstart', function (e) {
            if (!isMobile()) return;
            touchStartX = e.changedTouches[0].screenX;
            touchStartY = e.changedTouches[0].screenY;
        }, { passive: true });

        document.addEventListener('touchend', function (e) {
            if (!isMobile()) return;
            const touchEndX = e.changedTouches[0].screenX;
            const swipeDistanceX = touchEndX - touchStartX;
            const swipeDistanceY = Math.abs(e.changedTouches[0].screenY - touchStartY);

            if (swipeDistanceY > maxVerticalSwipe) return;

            if (swipeDistanceX > minSwipeDistance && touchStartX < 50 && !sidebar.classList.contains('active')) {
                openSidebar();
            } else if (swipeDistanceX < -minSwipeDistance && sidebar.classList.contains('active')) {
                closeSidebar();
            }
        }, { passive: true });

        let lastTouchY = 0;

        sidebar.addEventListener('touchstart', function (e) {
            lastTouchY = e.touches[0].clientY;
        }, { passive: true });

        sidebar.addEventListener('touchmove', function (e) {
            const sidebarNav = sidebar.querySelector('.sidebar-nav');
            if (!sidebarNav) return;

            const scrollTop = sidebarNav.scrollTop;
            const scrollHeight = sidebarNav.scrollHeight;
            const clientHeight = sidebarNav.clientHeight;
            const isAtTop = scrollTop === 0;
            const isAtBottom = scrollTop + clientHeight >= scrollHeight - 1;

            if ((isAtTop && e.touches[0].clientY > lastTouchY) ||
                (isAtBottom && e.touches[0].clientY < lastTouchY)) {
                return;
            }

            e.stopPropagation();
        }, { passive: false });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initSidebar);
    } else {
        initSidebar();
    }

    window.addEventListener('pageshow', function (event) {
        if (event.persisted) {
            initSidebar();
        }
    });
})();
