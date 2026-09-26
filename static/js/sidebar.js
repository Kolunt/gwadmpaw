// Управление сайдбаром
document.addEventListener('DOMContentLoaded', function() {
    const sidebar = document.getElementById('sidebar');
    const sidebarToggle = document.getElementById('sidebar-toggle');
    const sidebarClose = document.getElementById('sidebar-close');
    const sidebarOverlay = document.getElementById('sidebar-overlay');
    const sidebarCollapse = document.getElementById('sidebar-collapse');
    const SIDEBAR_COLLAPSED_KEY = 'gwadm-sidebar-collapsed';

    if (!sidebar || !sidebarToggle) {
        console.error('Sidebar elements not found');
        return;
    }

    const MOBILE_MAX = 1024;

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

    function updateLinkTitles(collapsed) {
        document.querySelectorAll('.sidebar-link').forEach(function(link) {
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
            return;
        }

        document.documentElement.classList.toggle('sidebar-collapsed', collapsed);
        if (sidebarCollapse) {
            sidebarCollapse.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
            sidebarCollapse.setAttribute('aria-label', collapsed ? 'Развернуть меню' : 'Свернуть меню');
        }
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

        let collapsed = false;
        try {
            collapsed = localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1';
        } catch (e) { /* ignore */ }
        setCollapsed(collapsed, false);
    }

    // Открытие сайдбара (drawer на mobile/tablet)
    function openSidebar() {
        if (!sidebar) return;
        sidebar.classList.add('active');
        if (sidebarOverlay) {
            sidebarOverlay.classList.add('active');
        }
        if (sidebarToggle) {
            sidebarToggle.classList.add('active');
        }
        document.body.style.overflow = 'hidden';
    }

    // Закрытие сайдбара (drawer на mobile/tablet)
    function closeSidebar() {
        if (!sidebar) return;
        sidebar.classList.remove('active');
        if (sidebarOverlay) {
            sidebarOverlay.classList.remove('active');
        }
        if (sidebarToggle) {
            sidebarToggle.classList.remove('active');
        }
        document.body.style.overflow = '';
    }

    sidebarToggle.addEventListener('click', function(e) {
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
        sidebarCollapse.addEventListener('click', function(e) {
            e.stopPropagation();
            if (!isMobile()) {
                toggleCollapsed();
            }
        });
    }

    if (sidebarClose) {
        sidebarClose.addEventListener('click', function(e) {
            e.stopPropagation();
            closeSidebar();
        });
    }

    if (sidebarOverlay) {
        sidebarOverlay.addEventListener('click', function(e) {
            e.stopPropagation();
            closeSidebar();
        });
    }

    const sidebarLinks = document.querySelectorAll('.sidebar-link');
    sidebarLinks.forEach(function(link) {
        link.addEventListener('click', function() {
            if (isMobile()) {
                setTimeout(closeSidebar, 100);
            }
        });
    });

    window.addEventListener('resize', function() {
        if (!isMobile()) {
            closeSidebar();
            applyStoredCollapseState();
            return;
        }
        setCollapsed(false, false);
    });

    applyStoredCollapseState();

    if (isMobile()) {
        closeSidebar();
    }

    // Поддержка свайпа для открытия/закрытия сайдбара (слева-направо)
    let touchStartX = 0;
    let touchStartY = 0;
    const minSwipeDistance = 50;
    const maxVerticalSwipe = 30;

    document.addEventListener('touchstart', function(e) {
        if (!isMobile()) return;
        touchStartX = e.changedTouches[0].screenX;
        touchStartY = e.changedTouches[0].screenY;
    }, { passive: true });

    document.addEventListener('touchend', function(e) {
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

    if (sidebar) {
        let lastTouchY = 0;

        sidebar.addEventListener('touchstart', function(e) {
            lastTouchY = e.touches[0].clientY;
        }, { passive: true });

        sidebar.addEventListener('touchmove', function(e) {
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
});
