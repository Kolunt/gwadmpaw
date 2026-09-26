(function () {
    'use strict';

    var phrases = window.HERO_WHISPERS;
    if (!phrases || !phrases.length) {
        return;
    }

    if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
        return;
    }

    var container = document.querySelector('.hero-whispers');
    var heroSection = document.querySelector('.hero-section');
    if (!container || !heroSection) {
        return;
    }

    var MAX_ACTIVE = window.innerWidth <= 768 ? 2 : 4;
    var activeCount = 0;
    var scheduleTimer = null;

    function randomBetween(min, max) {
        return min + Math.random() * (max - min);
    }

    function pickPhrase() {
        return phrases[Math.floor(Math.random() * phrases.length)];
    }

    function getAvoidRects() {
        var rects = [];
        var heroContent = heroSection.querySelector('.hero-content');
        var statsBar = heroSection.querySelector('.stats-bar-hero');
        if (heroContent) {
            rects.push(heroContent.getBoundingClientRect());
        }
        if (statsBar) {
            rects.push(statsBar.getBoundingClientRect());
        }
        return rects;
    }

    function overlaps(rect, avoidRects, padding) {
        for (var i = 0; i < avoidRects.length; i++) {
            var r = avoidRects[i];
            if (
                rect.left < r.right + padding &&
                rect.right > r.left - padding &&
                rect.top < r.bottom + padding &&
                rect.bottom > r.top - padding
            ) {
                return true;
            }
        }
        return false;
    }

    function placeWhisper(el) {
        var heroRect = heroSection.getBoundingClientRect();
        var avoidRects = getAvoidRects();
        var padding = 16;
        var maxAttempts = 24;
        var placed = false;

        for (var attempt = 0; attempt < maxAttempts; attempt++) {
            el.style.visibility = 'hidden';
            el.style.left = '0';
            el.style.top = '0';
            container.appendChild(el);

            var elRect = el.getBoundingClientRect();
            var maxLeft = heroRect.width - elRect.width - padding;
            var maxTop = heroRect.height - elRect.height - padding;
            if (maxLeft < padding || maxTop < padding) {
                break;
            }

            var left = padding + Math.random() * maxLeft;
            var top = padding + Math.random() * maxTop;

            el.style.left = left + 'px';
            el.style.top = top + 'px';

            elRect = el.getBoundingClientRect();
            if (!overlaps(elRect, avoidRects, 12)) {
                placed = true;
                break;
            }
        }

        if (!placed) {
            el.remove();
            return false;
        }

        el.style.visibility = '';
        return true;
    }

    function spawnWhisper() {
        if (activeCount >= MAX_ACTIVE) {
            return;
        }

        var el = document.createElement('div');
        el.className = 'hero-whisper';
        el.textContent = pickPhrase();

        var duration = randomBetween(6, 10);
        el.style.animationDuration = duration + 's';

        if (!placeWhisper(el)) {
            return;
        }

        activeCount += 1;

        el.addEventListener('animationend', function () {
            el.remove();
            activeCount -= 1;
        });
    }

    function scheduleNext() {
        if (scheduleTimer) {
            clearTimeout(scheduleTimer);
        }
        var delay = randomBetween(2000, 5000);
        scheduleTimer = setTimeout(function () {
            spawnWhisper();
            scheduleNext();
        }, delay);
    }

    function init() {
        spawnWhisper();
        setTimeout(spawnWhisper, randomBetween(800, 2000));
        scheduleNext();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
