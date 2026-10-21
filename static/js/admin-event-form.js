document.addEventListener('DOMContentLoaded', function () {
    var form = document.querySelector('.event-form');
    if (!form) {
        return;
    }

    var wraps = form.querySelectorAll('.datetime-field-wrap');
    wraps.forEach(function (wrap) {
        var input = wrap.querySelector('input.datetime-local-input');
        var btn = wrap.querySelector('.datetime-picker-btn');
        if (!input || !btn) {
            return;
        }

        btn.addEventListener('click', function () {
            if (typeof input.showPicker === 'function') {
                try {
                    input.showPicker();
                    return;
                } catch (e) {
                    /* showPicker may throw without user gesture or in unsupported builds */
                }
            }
            input.focus();
            input.click();
        });
    });
});
