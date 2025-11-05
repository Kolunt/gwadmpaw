// Service Worker для PWA
const CACHE_NAME = 'gwadmpaw-v1.16.1';
const urlsToCache = [
  '/',
  '/static/css/style.css',
  '/static/js/theme.js',
  '/static/js/sidebar.js',
  '/static/manifest.json'
];

// Установка Service Worker
self.addEventListener('install', function(event) {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(function(cache) {
        return cache.addAll(urlsToCache);
      })
      .catch(function(error) {
        console.log('Cache install failed:', error);
      })
  );
  self.skipWaiting();
});

// Активация Service Worker
self.addEventListener('activate', function(event) {
  event.waitUntil(
    caches.keys().then(function(cacheNames) {
      return Promise.all(
        cacheNames.map(function(cacheName) {
          if (cacheName !== CACHE_NAME) {
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
  return self.clients.claim();
});

// Перехват запросов
self.addEventListener('fetch', function(event) {
  // Не кэшируем API запросы и авторизацию
  if (event.request.url.includes('/api/') || 
      event.request.url.includes('/login') ||
      event.request.url.includes('/logout') ||
      event.request.url.includes('/admin/')) {
    return;
  }

  event.respondWith(
    caches.match(event.request)
      .then(function(response) {
        // Возвращаем из кэша или делаем запрос
        if (response) {
          return response;
        }
        return fetch(event.request).then(function(response) {
          // Проверяем валидность ответа
          if (!response || response.status !== 200 || response.type !== 'basic') {
            return response;
          }
          // Клонируем ответ для кэширования
          const responseToCache = response.clone();
          caches.open(CACHE_NAME)
            .then(function(cache) {
              cache.put(event.request, responseToCache);
            });
          return response;
        });
      })
      .catch(function() {
        // Если сеть недоступна, возвращаем офлайн страницу
        if (event.request.destination === 'document') {
          return caches.match('/');
        }
      })
  );
});

