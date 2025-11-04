// Управление сайдбаром
document.addEventListener('DOMContentLoaded', function() {
    const sidebar = document.getElementById('sidebar');
    const sidebarToggle = document.getElementById('sidebar-toggle');
    const sidebarClose = document.getElementById('sidebar-close');
    const sidebarOverlay = document.getElementById('sidebar-overlay');
    
    if (!sidebar || !sidebarToggle) {
        console.error('Sidebar elements not found');
        return;
    }
    
    // Проверка, мобильная ли версия
    function isMobile() {
        return window.innerWidth <= 768;
    }
    
    // Открытие сайдбара
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
    
    // Закрытие сайдбара
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
    
    // Обработчики событий
    sidebarToggle.addEventListener('click', function(e) {
        e.stopPropagation();
        if (sidebar.classList.contains('active')) {
            closeSidebar();
        } else {
            openSidebar();
        }
    });
    
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
    
    // Закрытие при клике на ссылку в мобильной версии
    const sidebarLinks = document.querySelectorAll('.sidebar-link');
    sidebarLinks.forEach(link => {
        link.addEventListener('click', function() {
            // На мобильных устройствах закрываем сайдбар после клика
            if (isMobile()) {
                setTimeout(closeSidebar, 100); // Небольшая задержка для плавности
            }
        });
    });
    
    // Закрытие при изменении размера окна (если перешли на десктоп)
    window.addEventListener('resize', function() {
        if (!isMobile()) {
            // На десктопе сайдбар всегда виден, убираем класс active
            sidebar.classList.remove('active');
            if (sidebarOverlay) {
                sidebarOverlay.classList.remove('active');
            }
            if (sidebarToggle) {
                sidebarToggle.classList.remove('active');
            }
            document.body.style.overflow = '';
        }
    });
    
    // Инициализация: на мобильных сайдбар должен быть скрыт
    if (isMobile()) {
        closeSidebar();
    }
});

