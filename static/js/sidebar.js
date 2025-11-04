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
    
    // Поддержка свайпа для открытия/закрытия сайдбара (слева-направо)
    let touchStartX = 0;
    let touchStartY = 0;
    let touchEndX = 0;
    let touchEndY = 0;
    const minSwipeDistance = 50;
    const maxVerticalSwipe = 30; // Максимальное вертикальное движение для горизонтального свайпа
    
    document.addEventListener('touchstart', function(e) {
        if (!isMobile()) return;
        touchStartX = e.changedTouches[0].screenX;
        touchStartY = e.changedTouches[0].screenY;
    }, { passive: true });
    
    document.addEventListener('touchend', function(e) {
        if (!isMobile()) return;
        touchEndX = e.changedTouches[0].screenX;
        touchEndY = e.changedTouches[0].screenY;
        
        const swipeDistanceX = touchEndX - touchStartX;
        const swipeDistanceY = Math.abs(e.changedTouches[0].screenY - touchStartY);
        
        // Проверяем, что это горизонтальный свайп (не вертикальный)
        if (swipeDistanceY > maxVerticalSwipe) return;
        
        // Свайп вправо для открытия (только если сайдбар закрыт и свайп начался слева)
        if (swipeDistanceX > minSwipeDistance && touchStartX < 50 && !sidebar.classList.contains('active')) {
            openSidebar();
        }
        // Свайп влево для закрытия (только если сайдбар открыт)
        else if (swipeDistanceX < -minSwipeDistance && sidebar.classList.contains('active')) {
            closeSidebar();
        }
    }, { passive: true });
    
    // Улучшенная прокрутка сайдбара на touch-устройствах
    if (sidebar) {
        let lastTouchY = 0;
        let isScrolling = false;
        
        sidebar.addEventListener('touchstart', function(e) {
            lastTouchY = e.touches[0].clientY;
            isScrolling = false;
        }, { passive: true });
        
        sidebar.addEventListener('touchmove', function(e) {
            if (!isScrolling) {
                const currentY = e.touches[0].clientY;
                const deltaY = Math.abs(currentY - lastTouchY);
                
                // Если движение больше 5px, считаем это прокруткой
                if (deltaY > 5) {
                    isScrolling = true;
                }
            }
        }, { passive: true });
        
        // Предотвращаем прокрутку страницы при прокрутке сайдбара
        sidebar.addEventListener('touchmove', function(e) {
            const sidebarNav = sidebar.querySelector('.sidebar-nav');
            if (sidebarNav) {
                const scrollTop = sidebarNav.scrollTop;
                const scrollHeight = sidebarNav.scrollHeight;
                const clientHeight = sidebarNav.clientHeight;
                const isAtTop = scrollTop === 0;
                const isAtBottom = scrollTop + clientHeight >= scrollHeight - 1;
                
                if ((isAtTop && e.touches[0].clientY > lastTouchY) || 
                    (isAtBottom && e.touches[0].clientY < lastTouchY)) {
                    // Прокрутка достигла границы, позволяем прокрутку страницы
                    return;
                }
                
                // Иначе предотвращаем прокрутку страницы
                e.stopPropagation();
            }
        }, { passive: false });
    }
});

