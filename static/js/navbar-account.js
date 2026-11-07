(function () {
    'use strict';

    function closeAccountMenus(except) {
        document.querySelectorAll('.navbar-account[open]').forEach(function (el) {
            if (el !== except) {
                el.removeAttribute('open');
            }
        });
    }

    document.addEventListener('click', function (event) {
        var account = event.target.closest && event.target.closest('.navbar-account');
        if (!account) {
            closeAccountMenus(null);
        }
    });

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') {
            closeAccountMenus(null);
        }
    });
})();
