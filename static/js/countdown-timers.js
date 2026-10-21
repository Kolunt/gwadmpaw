/**
 * Countdown timers: parent element must have data-start (ISO or "YYYY-MM-DD HH:MM:SS").
 */
function initCountdownTimers(selector) {
    const elements = document.querySelectorAll(selector);
    elements.forEach(function (element) {
        const targetIso = element.getAttribute('data-start');
        if (!targetIso) {
            return;
        }

        const targetDate = new Date(targetIso.replace(' ', 'T'));
        if (isNaN(targetDate.getTime())) {
            return;
        }

        const values = {
            days: element.querySelector('[data-unit="days"]'),
            hours: element.querySelector('[data-unit="hours"]'),
            minutes: element.querySelector('[data-unit="minutes"]'),
            seconds: element.querySelector('[data-unit="seconds"]'),
        };

        if (!values.days || !values.hours || !values.minutes || !values.seconds) {
            return;
        }

        let intervalId = null;

        function updateCountdown() {
            const now = new Date();
            const diff = targetDate - now;

            if (diff <= 0) {
                values.days.textContent = '0';
                values.hours.textContent = '00';
                values.minutes.textContent = '00';
                values.seconds.textContent = '00';
                element.classList.add('countdown-complete');
                if (intervalId !== null) {
                    clearInterval(intervalId);
                }
                return;
            }

            const seconds = Math.floor(diff / 1000) % 60;
            const minutes = Math.floor(diff / (1000 * 60)) % 60;
            const hours = Math.floor(diff / (1000 * 60 * 60)) % 24;
            const days = Math.floor(diff / (1000 * 60 * 60 * 24));

            values.days.textContent = String(days);
            values.hours.textContent = String(hours).padStart(2, '0');
            values.minutes.textContent = String(minutes).padStart(2, '0');
            values.seconds.textContent = String(seconds).padStart(2, '0');
        }

        updateCountdown();
        intervalId = setInterval(updateCountdown, 1000);
    });
}

if (typeof window !== 'undefined') {
    window.initCountdownTimers = initCountdownTimers;
}
