"""
Скрипт для генерации иконок PWA из эмодзи
Требует: pip install Pillow
"""
try:
    from PIL import Image, ImageDraw, ImageFont
    import os
except ImportError:
    print("Pillow не установлен. Установите: pip install Pillow")
    exit(1)

# Создаем папку для иконок
icons_dir = os.path.join('static', 'icons')
os.makedirs(icons_dir, exist_ok=True)

# Размеры иконок для PWA
sizes = [72, 96, 128, 144, 152, 192, 384, 512]

# Эмодзи для иконки
emoji = "🎅"

for size in sizes:
    # Создаем изображение с прозрачным фоном
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    # Рисуем круглый фон
    margin = size // 10
    draw.ellipse([margin, margin, size - margin, size - margin], 
                 fill=(255, 255, 255, 255), outline=(0, 0, 0, 0))
    
    # Пытаемся использовать системный шрифт для эмодзи
    try:
        # Для Windows
        font_path = "C:/Windows/Fonts/seguiemj.ttf"
        if not os.path.exists(font_path):
            # Для Linux/Mac
            font_path = "/System/Library/Fonts/Apple Color Emoji.ttc"
            if not os.path.exists(font_path):
                font_path = None
    except:
        font_path = None
    
    if font_path and os.path.exists(font_path):
        try:
            font = ImageFont.truetype(font_path, size=int(size * 0.6))
        except:
            font = None
    else:
        font = None
    
    # Рисуем эмодзи (используем шрифт если доступен)
    if font:
        # Получаем размер текста
        bbox = draw.textbbox((0, 0), emoji, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        # Центрируем текст
        x = (size - text_width) // 2
        y = (size - text_height) // 2 - bbox[1]
        
        draw.text((x, y), emoji, font=font, fill=(0, 0, 0, 255))
    else:
        # Простое решение: рисуем круг
        center = size // 2
        radius = size // 3
        draw.ellipse([center - radius, center - radius, center + radius, center + radius],
                     fill=(220, 53, 69, 255))
        # Рисуем простую звезду
        points = []
        for i in range(5):
            angle = (i * 144 - 90) * 3.14159 / 180
            x = center + int(radius * 0.8 * (1 if i % 2 == 0 else 0.4) * (1 if i % 2 == 0 else 1) * (1 if i % 2 == 0 else 0.5))
            y = center + int(radius * 0.8 * (1 if i % 2 == 0 else 0.4) * (1 if i % 2 == 0 else 1) * (1 if i % 2 == 0 else 0.5))
            if i % 2 == 0:
                x = center + int(radius * 0.8 * (1 if i % 2 == 0 else 0.4) * (1 if i % 2 == 0 else 1) * (1 if i % 2 == 0 else 0.5))
                y = center + int(radius * 0.8 * (1 if i % 2 == 0 else 0.4) * (1 if i % 2 == 0 else 1) * (1 if i % 2 == 0 else 0.5))
        # Проще: рисуем красный круг с белой звездой
        draw.ellipse([center - radius, center - radius, center + radius, center + radius],
                     fill=(220, 53, 69, 255))
        # Рисуем белую звезду (упрощенную)
        star_size = radius // 2
        for i in range(5):
            angle = (i * 72 - 90) * 3.14159 / 180
            x = center + int((radius * 0.7) * (1 if i % 2 == 0 else 0.3) * (1 if i % 2 == 0 else 1) * (1 if i % 2 == 0 else 0.5))
            y = center + int((radius * 0.7) * (1 if i % 2 == 0 else 0.3) * (1 if i % 2 == 0 else 1) * (1 if i % 2 == 0 else 0.5))
    
    # Сохраняем иконку
    filename = f'icon-{size}x{size}.png'
    filepath = os.path.join(icons_dir, filename)
    img.save(filepath, 'PNG')
    print(f'Создана иконка: {filepath}')

print(f'\nИконки созданы в папке: {icons_dir}')

