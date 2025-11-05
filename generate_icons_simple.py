"""
Простой скрипт для генерации SVG иконок PWA
Не требует внешних библиотек
"""
import os

# Создаем папку для иконок
icons_dir = os.path.join('static', 'icons')
os.makedirs(icons_dir, exist_ok=True)

# Размеры иконок для PWA
sizes = [72, 96, 128, 144, 152, 192, 384, 512]

# SVG шаблон для иконки
def create_icon_svg(size):
    emoji_size = int(size * 0.6)
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<svg width="{size}" height="{size}" xmlns="http://www.w3.org/2000/svg">
  <rect width="{size}" height="{size}" rx="{size//4}" fill="#dc3545"/>
  <text x="50%" y="50%" font-size="{emoji_size}" text-anchor="middle" dominant-baseline="central" font-family="Arial, sans-serif">🎅</text>
</svg>'''

for size in sizes:
    filename = f'icon-{size}x{size}.svg'
    filepath = os.path.join(icons_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(create_icon_svg(size))
    
    print(f'Создана иконка: {filepath}')

print(f'\nSVG иконки созданы в папке: {icons_dir}')
print('Для PWA лучше использовать PNG. Конвертируйте SVG в PNG через онлайн-инструмент или установите Pillow.')

