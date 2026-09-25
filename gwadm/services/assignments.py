"""Letter assignments and event pairing."""

from gwadm.db import get_db_connection
from gwadm.logging_config import log_debug, log_error
import os
from werkzeug.utils import secure_filename

from gwadm.config import ASSIGNMENT_RECEIPT_FOLDER, LETTER_UPLOAD_FOLDER, ALLOWED_LETTER_IMAGE_EXTENSIONS
from gwadm.services.activity import log_activity

def create_random_assignments(event_id, assigned_by):
    """Создает случайное распределение Деда Мороза и Внучки"""
    conn = get_db_connection()
    try:
        # Получаем утвержденных участников
        participants = get_approved_participants(event_id)
        
        if len(participants) < 2:
            return False, "Недостаточно утвержденных участников (нужно минимум 2)"
        
        # Создаем список ID участников
        participant_ids = [p['user_id'] for p in participants]
        
        # Перемешиваем список
        random.shuffle(participant_ids)
        
        # Создаем циклическое распределение (каждый дарит следующему)
        assignments = []
        for i in range(len(participant_ids)):
            santa_id = participant_ids[i]
            recipient_id = participant_ids[(i + 1) % len(participant_ids)]  # Циклическое распределение
            assignments.append((santa_id, recipient_id))
        
        success, result = save_event_assignments(event_id, assignments, assigned_by, connection=conn)
        if success:
            return True, f"Создано {result} заданий"
        return False, result
    except Exception as e:
        log_error(f"Error creating random assignments: {e}")
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()


def save_event_assignments(event_id, assignments, assigned_by, locked_pairs=None, assignment_locked=False, connection=None):
    """Сохраняет распределение пар"""
    conn = connection or get_db_connection()
    try:
        existing_rows = conn.execute('''
            SELECT santa_user_id, recipient_user_id, locked, assignment_locked, santa_sent_at, santa_send_info, recipient_received_at, recipient_thanks_message, recipient_receipt_image, assigned_at, assigned_by
            FROM event_assignments
            WHERE event_id = ?
        ''', (event_id,)).fetchall()
        
        # Создаем словарь для быстрого поиска старых данных по паре (santa, recipient)
        existing_data_map = {}
        for row in existing_rows:
            key = (row['santa_user_id'], row['recipient_user_id'])
            existing_data_map[key] = {
                'locked': row['locked'] if 'locked' in row.keys() else 0,
                'assignment_locked': row['assignment_locked'] if 'assignment_locked' in row.keys() else 0,
                'santa_sent_at': row['santa_sent_at'] if 'santa_sent_at' in row.keys() else None,
                'santa_send_info': row['santa_send_info'] if 'santa_send_info' in row.keys() else None,
                'recipient_received_at': row['recipient_received_at'] if 'recipient_received_at' in row.keys() else None,
                'recipient_thanks_message': row['recipient_thanks_message'] if 'recipient_thanks_message' in row.keys() else None,
                'recipient_receipt_image': row['recipient_receipt_image'] if 'recipient_receipt_image' in row.keys() else None,
                'assigned_at': row['assigned_at'] if 'assigned_at' in row.keys() else None,
                'assigned_by': row['assigned_by'] if 'assigned_by' in row.keys() else None
            }
        
        # Получаем старые назначения для проверки наличия сообщений
        old_assignments_map = {}
        old_assignments_rows = conn.execute('''
            SELECT id, santa_user_id, recipient_user_id
            FROM event_assignments
            WHERE event_id = ?
        ''', (event_id,)).fetchall()
        for row in old_assignments_rows:
            key = (row['santa_user_id'], row['recipient_user_id'])
            old_assignments_map[key] = row['id']
        
        # Определяем, какие назначения нужно удалить
        # НЕ удаляем назначения, у которых есть сообщения (чаты) - они сохраняются навсегда для админа
        # Удаляем все старые назначения без сообщений, даже если для той же пары будет создано новое назначение
        new_pairs_set = set(assignments)
        
        # Проверяем все старые назначения и удаляем те, у которых нет сообщений
        # НЕ удаляем архивированные назначения - они уже в архиве
        deleted_count = 0
        kept_count = 0
        archived_count = 0
        for row in old_assignments_rows:
            santa_id = row['santa_user_id']
            recipient_id = row['recipient_user_id']
            assignment_id = row['id']
            
            # Проверяем, архивировано ли назначение
            assignment_check = conn.execute('''
                SELECT is_archived FROM event_assignments WHERE id = ?
            ''', (assignment_id,)).fetchone()
            is_archived = assignment_check and ('is_archived' in assignment_check.keys() and assignment_check['is_archived'] == 1)
            
            if is_archived:
                # Архивированное назначение - не трогаем
                archived_count += 1
                log_debug(f"Skipping archived assignment_id {assignment_id} for pair ({santa_id}, {recipient_id})")
                continue
            
            # Проверяем, есть ли сообщения у этого назначения
            message_count = conn.execute('''
                SELECT COUNT(*) as cnt FROM letter_messages WHERE assignment_id = ?
            ''', (assignment_id,)).fetchone()
            
            if message_count and message_count['cnt'] > 0:
                # Есть сообщения - НЕ удаляем, сохраняем чат навсегда
                kept_count += 1
                log_debug(f"Keeping assignment_id {assignment_id} for pair ({santa_id}, {recipient_id}) with {message_count['cnt']} messages - chat preserved for admin")
            else:
                # Нет сообщений - удаляем, даже если для этой пары будет создано новое назначение
                conn.execute('''
                    DELETE FROM event_assignments 
                    WHERE id = ?
                ''', (assignment_id,))
                deleted_count += 1
                log_debug(f"Deleted assignment_id {assignment_id} for pair ({santa_id}, {recipient_id}) - no messages")
        
        if deleted_count > 0:
            log_debug(f"Deleted {deleted_count} old assignments without messages")
        if kept_count > 0:
            log_debug(f"Kept {kept_count} old assignments with messages - chats preserved permanently for admin")
        if archived_count > 0:
            log_debug(f"Skipped {archived_count} archived assignments")
        
        locked_map = {}
        if locked_pairs:
            for entry in locked_pairs:
                if isinstance(entry, dict):
                    santa = entry.get('santa_id')
                    recipient = entry.get('recipient_id')
                else:
                    try:
                        santa, recipient = entry
                    except Exception:
                        continue
                try:
                    santa = int(santa)
                    recipient = int(recipient)
                except (TypeError, ValueError):
                    continue
                locked_map[santa] = recipient

        # Создаем новые назначения для всех пар из нового распределения
        # НЕ переносим сообщения - создаем новый чат, даже если для этой пары уже был чат
        # Но проверяем, не была ли эта пара уже расформирована (архивирована)
        data = []
        updated_count = 0
        for santa, recipient in assignments:
            # Проверяем, есть ли архивированное назначение для этой пары
            archived_check = conn.execute('''
                SELECT id FROM assignment_chat_history
                WHERE event_id = ? AND santa_user_id = ? AND recipient_user_id = ?
                ORDER BY archived_at DESC
                LIMIT 1
            ''', (event_id, santa, recipient)).fetchone()
            
            if archived_check:
                log_debug(f"Creating new assignment for pair ({santa}, {recipient}) - previous chat was archived (history_id: {archived_check['id']})")
            # Проверяем, есть ли старые данные для этой пары (для сохранения статуса отправки/получения)
            old_data = existing_data_map.get((santa, recipient), {})
            
            # Определяем locked статус
            # Замок устанавливается только если пара явно указана в locked_pairs
            # Если пара не в locked_pairs, то замок снимается (locked_flag = 0)
            locked_flag = 0
            if assignment_locked or locked_map.get(santa) == recipient:
                locked_flag = 1
            # НЕ сохраняем старый locked статус, если пара не в locked_pairs
            # Это позволяет снимать замки с пар
            
            assignment_locked_flag = 1 if assignment_locked else (old_data.get('assignment_locked', 0))
            
            # Сохраняем старые данные о отправке/получении, если они есть
            # НО: создаем новое назначение, даже если для этой пары уже было назначение
            santa_sent_at = old_data.get('santa_sent_at')
            santa_send_info = old_data.get('santa_send_info')
            recipient_received_at = old_data.get('recipient_received_at')
            recipient_thanks_message = old_data.get('recipient_thanks_message')
            recipient_receipt_image = old_data.get('recipient_receipt_image')
            assigned_at = datetime.now().isoformat()  # Всегда новая дата назначения для нового чата
            assigned_by_final = assigned_by  # Всегда новый назначивший для нового чата
            
            # Проверяем, существует ли уже активное (не архивированное) назначение для этой пары
            # ВАЖНО: проверяем ВСЕ записи, включая те, у которых есть сообщения (они не были удалены)
            existing_active = conn.execute('''
                SELECT id, is_archived FROM event_assignments
                WHERE event_id = ? AND santa_user_id = ? AND recipient_user_id = ? 
                  AND (is_archived = 0 OR is_archived IS NULL)
            ''', (event_id, santa, recipient)).fetchone()
            
            if existing_active:
                # Активное назначение уже существует - обновляем его вместо создания нового
                # Это предотвращает UNIQUE constraint violation
                existing_id = existing_active['id']
                conn.execute('''
                    UPDATE event_assignments
                    SET assigned_by = ?, locked = ?, assignment_locked = ?,
                        santa_sent_at = ?, santa_send_info = ?, recipient_received_at = ?,
                        recipient_thanks_message = ?, recipient_receipt_image = ?,
                        assigned_at = ?, is_archived = 0
                    WHERE id = ?
                ''', (
                    assigned_by_final, locked_flag, assignment_locked_flag,
                    santa_sent_at, santa_send_info, recipient_received_at,
                    recipient_thanks_message, recipient_receipt_image,
                    assigned_at, existing_id
                ))
                updated_count += 1
                log_debug(f"Updated existing assignment_id {existing_id} for pair ({santa}, {recipient})")
            else:
                # Новое назначение - добавляем в список для вставки
                data.append((
                    event_id, santa, recipient, assigned_by_final, locked_flag, assignment_locked_flag,
                    santa_sent_at, santa_send_info, recipient_received_at, recipient_thanks_message, recipient_receipt_image,
                    assigned_at
                ))
        
        # Вставляем новые назначения - всегда создаем новые, даже если для пары уже был чат
        # Но сначала еще раз проверяем, что записи не существует (на случай race condition)
        cursor = conn.cursor()
        for assignment_data in data:
            event_id_check, santa, recipient = assignment_data[0], assignment_data[1], assignment_data[2]
            
            # Дополнительная проверка перед вставкой - на случай если запись появилась между проверкой и вставкой
            final_check = conn.execute('''
                SELECT id FROM event_assignments
                WHERE event_id = ? AND santa_user_id = ? AND recipient_user_id = ? 
                  AND (is_archived = 0 OR is_archived IS NULL)
            ''', (event_id_check, santa, recipient)).fetchone()
            
            if final_check:
                # Запись появилась - обновляем её вместо создания новой
                existing_id = final_check['id']
                locked_flag = assignment_data[4]
                assignment_locked_flag = assignment_data[5]
                conn.execute('''
                    UPDATE event_assignments
                    SET assigned_by = ?, locked = ?, assignment_locked = ?,
                        santa_sent_at = ?, santa_send_info = ?, recipient_received_at = ?,
                        recipient_thanks_message = ?, recipient_receipt_image = ?,
                        assigned_at = ?, is_archived = 0
                    WHERE id = ?
                ''', (
                    assignment_data[3], locked_flag, assignment_locked_flag,
                    assignment_data[6], assignment_data[7], assignment_data[8],
                    assignment_data[9], assignment_data[10],
                    assignment_data[11], existing_id
                ))
                updated_count += 1
                log_debug(f"Updated existing assignment_id {existing_id} for pair ({santa}, {recipient}) - found during final check")
            else:
                # Записи нет - создаем новую
                try:
                    cursor.execute('''
                        INSERT INTO event_assignments (
                            event_id, santa_user_id, recipient_user_id, assigned_by, locked, assignment_locked,
                            santa_sent_at, santa_send_info, recipient_received_at, recipient_thanks_message, recipient_receipt_image,
                            assigned_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', assignment_data)
                    new_assignment_id = cursor.lastrowid
                    log_debug(f"Created new assignment_id {new_assignment_id} for pair ({santa}, {recipient}) - new chat created")
                except sqlite3.IntegrityError as e:
                    # Если все же возникла ошибка UNIQUE constraint - пытаемся обновить существующую запись
                    if 'UNIQUE constraint' in str(e):
                        log_debug(f"UNIQUE constraint error for pair ({santa}, {recipient}), attempting to update existing record")
                        existing_final = conn.execute('''
                            SELECT id FROM event_assignments
                            WHERE event_id = ? AND santa_user_id = ? AND recipient_user_id = ?
                        ''', (event_id_check, santa, recipient)).fetchone()
                        if existing_final:
                            existing_id = existing_final['id']
                            conn.execute('''
                                UPDATE event_assignments
                                SET assigned_by = ?, locked = ?, assignment_locked = ?,
                                    santa_sent_at = ?, santa_send_info = ?, recipient_received_at = ?,
                                    recipient_thanks_message = ?, recipient_receipt_image = ?,
                                    assigned_at = ?, is_archived = 0
                                WHERE id = ?
                            ''', (
                                assignment_data[3], assignment_data[4], assignment_data[5],
                                assignment_data[6], assignment_data[7], assignment_data[8],
                                assignment_data[9], assignment_data[10],
                                assignment_data[11], existing_id
                            ))
                            updated_count += 1
                            log_debug(f"Updated existing assignment_id {existing_id} for pair ({santa}, {recipient}) - after UNIQUE constraint error")
                        else:
                            raise  # Если записи нет, но ошибка UNIQUE - это странно, пробрасываем дальше
                    else:
                        raise  # Другие ошибки пробрасываем дальше
        
        # НЕ переносим сообщения - каждый раз создается новый чат
        
        conn.commit()
        log_activity(
            'assignments_saved',
            details=f'Сохранено распределение для мероприятия #{event_id}',
            metadata={
                'event_id': event_id,
                'pairs_count': len(assignments),
                'assigned_by': assigned_by,
            },
            user_id=assigned_by
        )
        return True, len(assignments)
    except Exception as e:
        log_error(f"Error saving assignments for event {event_id}: {e}")
        conn.rollback()
        try:
            conn.execute('DELETE FROM event_assignments WHERE event_id = ?', (event_id,))
            conn.executemany('''
                INSERT INTO event_assignments (
                    event_id,
                    santa_user_id,
                    recipient_user_id,
                    locked,
                    assignment_locked,
                    santa_sent_at,
                    santa_send_info,
                    recipient_received_at,
                    recipient_thanks_message,
                    recipient_receipt_image,
                    assigned_at,
                    assigned_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (
                    event_id,
                    row['santa_user_id'],
                    row['recipient_user_id'],
                    row['locked'] if 'locked' in row.keys() else 0,
                    row['assignment_locked'] if 'assignment_locked' in row.keys() else 0,
                    row['santa_sent_at'] if 'santa_sent_at' in row.keys() else None,
                    row['santa_send_info'] if 'santa_send_info' in row.keys() else None,
                    row['recipient_received_at'] if 'recipient_received_at' in row.keys() else None,
                    row['recipient_thanks_message'] if 'recipient_thanks_message' in row.keys() else None,
                    row['recipient_receipt_image'] if 'recipient_receipt_image' in row.keys() else None,
                    row['assigned_at'] if 'assigned_at' in row.keys() else None,
                    row['assigned_by'] if 'assigned_by' in row.keys() else None
                )
                for row in existing_rows
            ])
            conn.commit()
        except Exception as restore_error:
            log_error(f"Failed to restore previous assignments for event {event_id}: {restore_error}")
        return False, str(e)


def get_user_assignments(user_id):
    """Получает задания пользователя (где он Дед Мороз и где Внучка)
    Показывает только последнее назначение для каждой пары в рамках мероприятия
    """
    conn = get_db_connection()
    # Получаем задания, где пользователь Дед Мороз
    # Используем подзапрос для получения только последнего назначения для каждой пары
    as_santa_rows = conn.execute('''
        SELECT 
            ea.*,
            e.name AS event_name,
            e.id AS event_id,
            recipient.username AS recipient_username,
            recipient.level AS recipient_level,
            recipient.synd AS recipient_synd,
            COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
            COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
            COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
            COALESCE(rd.postal_code, recipient.postal_code) AS recipient_postal_code,
            COALESCE(rd.country, recipient.country) AS recipient_country,
            COALESCE(rd.city, recipient.city) AS recipient_city,
            COALESCE(rd.street, recipient.street) AS recipient_street,
            COALESCE(rd.house, recipient.house) AS recipient_house,
            COALESCE(rd.building, recipient.building) AS recipient_building,
            COALESCE(rd.apartment, recipient.apartment) AS recipient_apartment,
            COALESCE(rd.email, recipient.email) AS recipient_email,
            COALESCE(rd.phone, recipient.phone) AS recipient_phone,
            COALESCE(rd.telegram, recipient.telegram) AS recipient_telegram,
            COALESCE(rd.whatsapp, recipient.whatsapp) AS recipient_whatsapp,
            COALESCE(rd.viber, recipient.viber) AS recipient_viber,
            rd.bio AS recipient_bio
        FROM event_assignments ea
        JOIN events e ON ea.event_id = e.id
        JOIN users recipient ON ea.recipient_user_id = recipient.user_id
        LEFT JOIN event_registration_details rd
            ON rd.event_id = ea.event_id AND rd.user_id = ea.recipient_user_id
        WHERE ea.santa_user_id = ?
          AND ea.is_archived = 0
          AND ea.id = (
              SELECT id FROM event_assignments ea2
              WHERE ea2.event_id = ea.event_id
                AND ea2.santa_user_id = ea.santa_user_id
                AND ea2.recipient_user_id = ea.recipient_user_id
                AND ea2.is_archived = 0
              ORDER BY ea2.assigned_at DESC, ea2.id DESC
              LIMIT 1
          )
        ORDER BY ea.assigned_at DESC
    ''', (user_id,)).fetchall()
    
    # Получаем задания, где пользователь Внучка
    as_recipient_rows = conn.execute('''
        SELECT 
            ea.*,
            e.name as event_name,
            e.id as event_id,
            santa.username as santa_username,
            santa.level as santa_level,
            santa.synd as santa_synd,
            recipient.username AS recipient_username,
            recipient.level AS recipient_level,
            recipient.synd AS recipient_synd,
            COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
            COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
            COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
            COALESCE(rd.postal_code, recipient.postal_code) AS recipient_postal_code,
            COALESCE(rd.country, recipient.country) AS recipient_country,
            COALESCE(rd.city, recipient.city) AS recipient_city,
            COALESCE(rd.street, recipient.street) AS recipient_street,
            COALESCE(rd.house, recipient.house) AS recipient_house,
            COALESCE(rd.building, recipient.building) AS recipient_building,
            COALESCE(rd.apartment, recipient.apartment) AS recipient_apartment,
            COALESCE(rd.email, recipient.email) AS recipient_email,
            COALESCE(rd.phone, recipient.phone) AS recipient_phone,
            COALESCE(rd.telegram, recipient.telegram) AS recipient_telegram,
            COALESCE(rd.whatsapp, recipient.whatsapp) AS recipient_whatsapp,
            COALESCE(rd.viber, recipient.viber) AS recipient_viber,
            rd.bio AS recipient_bio
        FROM event_assignments ea
        JOIN events e ON ea.event_id = e.id
        JOIN users santa ON ea.santa_user_id = santa.user_id
        JOIN users recipient ON ea.recipient_user_id = recipient.user_id
        LEFT JOIN event_registration_details rd
            ON rd.event_id = ea.event_id AND rd.user_id = ea.recipient_user_id
        WHERE ea.recipient_user_id = ?
          AND ea.is_archived = 0
          AND ea.id = (
              SELECT id FROM event_assignments ea2
              WHERE ea2.event_id = ea.event_id
                AND ea2.santa_user_id = ea.santa_user_id
                AND ea2.recipient_user_id = ea.recipient_user_id
                AND ea2.is_archived = 0
              ORDER BY ea2.assigned_at DESC, ea2.id DESC
              LIMIT 1
          )
        ORDER BY ea.assigned_at DESC
    ''', (user_id,)).fetchall()
    
    assignments = []
    send_info_updates = []
    thanks_updates = []
    
    for row in as_santa_rows:
        record = dict(row)
        info = record.get('santa_send_info')
        if info:
            normalized = _normalize_multiline_text(info)
            if normalized != info:
                record['santa_send_info'] = normalized
                send_info_updates.append((normalized, record['id']))
        assignments.append(record)
    
    for row in as_recipient_rows:
        record = dict(row)
        send_info = record.get('santa_send_info')
        if send_info:
            normalized = _normalize_multiline_text(send_info)
            if normalized != send_info:
                record['santa_send_info'] = normalized
                send_info_updates.append((normalized, record['id']))
        thanks = record.get('recipient_thanks_message')
        if thanks:
            normalized_thanks = _normalize_multiline_text(thanks)
            if normalized_thanks != thanks:
                record['recipient_thanks_message'] = normalized_thanks
                thanks_updates.append((normalized_thanks, record['id']))
        assignments.append(record)
    
    if send_info_updates:
        conn.executemany('UPDATE event_assignments SET santa_send_info = ? WHERE id = ?', send_info_updates)
    if thanks_updates:
        conn.executemany('UPDATE event_assignments SET recipient_thanks_message = ? WHERE id = ?', thanks_updates)
    if send_info_updates or thanks_updates:
        conn.commit()
    conn.close()
    
    return assignments


def get_admin_letter_assignments():
    """Возвращает все переписки для администраторов"""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT
            ea.*,
            e.name AS event_name,
            santa.username AS santa_username,
            santa.first_name AS santa_first_name,
            santa.last_name AS santa_last_name,
            santa.middle_name AS santa_middle_name,
            COALESCE(sd.country, santa.country) AS santa_country,
            COALESCE(sd.city, santa.city) AS santa_city,
            recipient.username AS recipient_username,
            COALESCE(rd.last_name, recipient.last_name) AS recipient_last_name,
            COALESCE(rd.first_name, recipient.first_name) AS recipient_first_name,
            COALESCE(rd.middle_name, recipient.middle_name) AS recipient_middle_name,
            COALESCE(rd.postal_code, recipient.postal_code) AS recipient_postal_code,
            COALESCE(rd.country, recipient.country) AS recipient_country,
            COALESCE(rd.city, recipient.city) AS recipient_city,
            COALESCE(rd.street, recipient.street) AS recipient_street,
            COALESCE(rd.house, recipient.house) AS recipient_house,
            COALESCE(rd.building, recipient.building) AS recipient_building,
            COALESCE(rd.apartment, recipient.apartment) AS recipient_apartment,
            rd.bio AS recipient_bio,
            lm.message_count,
            lm.last_message_at
        FROM event_assignments ea
        JOIN events e ON ea.event_id = e.id
        JOIN users santa ON ea.santa_user_id = santa.user_id
        JOIN users recipient ON ea.recipient_user_id = recipient.user_id
        LEFT JOIN event_registration_details rd
            ON rd.event_id = ea.event_id AND rd.user_id = ea.recipient_user_id
        LEFT JOIN event_registration_details sd
            ON sd.event_id = ea.event_id AND sd.user_id = ea.santa_user_id
        LEFT JOIN (
            SELECT assignment_id,
                   COUNT(*) AS message_count,
                   MAX(created_at) AS last_message_at
            FROM letter_messages
            GROUP BY assignment_id
        ) lm ON lm.assignment_id = ea.id
        WHERE ea.is_archived = 0
        ORDER BY
            CASE WHEN lm.last_message_at IS NULL THEN 1 ELSE 0 END,
            lm.last_message_at DESC,
            ea.id ASC
    ''').fetchall()

    assignments = []
    send_info_updates = []
    thanks_updates = []
    for row in rows:
        record = dict(row)
        info = record.get('santa_send_info')
        if info:
            normalized = _normalize_multiline_text(info)
            if normalized != info:
                record['santa_send_info'] = normalized
                send_info_updates.append((normalized, record['id']))
        thanks = record.get('recipient_thanks_message')
        if thanks:
            normalized_thanks = _normalize_multiline_text(thanks)
            if normalized_thanks != thanks:
                record['recipient_thanks_message'] = normalized_thanks
                thanks_updates.append((normalized_thanks, record['id']))
        record['message_count'] = record.get('message_count') or 0
        record['last_message_at'] = record.get('last_message_at')
        santa_parts = [record.get('santa_last_name') or '', record.get('santa_first_name') or '', record.get('santa_middle_name') or '']
        record['santa_full_name'] = ' '.join(part for part in santa_parts if part).strip() or record.get('santa_username')
        recipient_parts = [record.get('recipient_last_name') or '', record.get('recipient_first_name') or '', record.get('recipient_middle_name') or '']
        record['recipient_full_name'] = ' '.join(part for part in recipient_parts if part).strip() or record.get('recipient_username')
        record['chat_role'] = 'admin'
        assignments.append(record)

    if send_info_updates:
        conn.executemany('UPDATE event_assignments SET santa_send_info = ? WHERE id = ?', send_info_updates)
    if thanks_updates:
        conn.executemany('UPDATE event_assignments SET recipient_thanks_message = ? WHERE id = ?', thanks_updates)
    if send_info_updates or thanks_updates:
        conn.commit()
    conn.close()
    return assignments


def mark_assignment_sent(assignment_id, user_id, send_info):
    """Отмечает, что подарок отправлен"""
    clear_requested = False
    if not send_info or not send_info.strip():
        return False, 'Введите данные об отправке'
    send_info = _normalize_multiline_text(send_info, max_length=500)
    if not send_info:
        return False, 'Введите данные об отправке'
    
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        return False, 'Некорректный идентификатор пользователя'
    
    conn = get_db_connection()
    assignment = conn.execute('SELECT * FROM event_assignments WHERE id = ?', (assignment_id,)).fetchone()
    
    if not assignment:
        conn.close()
        return False, 'Задание не найдено'
    
    if assignment['santa_user_id'] != user_id_int:
        conn.close()
        return False, 'Вы не можете обновить это задание'
    
    if is_event_finished(assignment['event_id']):
        conn.close()
        return False, 'Мероприятие завершено. Действия с заданием недоступны.'

    try:
        chat_message = None
        if clear_requested:
            conn.execute('''
                UPDATE event_assignments
                SET santa_send_info = NULL
                WHERE id = ?
            ''', (assignment_id,))
            system_message = (
                "Дорогой внучок! Я скорректировал информацию об отправке. "
                "Если будут вопросы — пиши!"
            )
            conn.execute('''
                INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
                VALUES (?, 'santa', ?, NULL)
            ''', (assignment_id, system_message))
        else:
            chat_message = (
                f"Дорогой внучок! Я всё отправил! {send_info}\n"
                "Если будут вопросы — пиши!"
            ).strip()
        previous_info = assignment['santa_send_info']
        updated_existing = bool(previous_info)
        was_already_sent = bool(assignment['santa_sent_at'])
        
        conn.execute('''
            UPDATE event_assignments
            SET santa_sent_at = CURRENT_TIMESTAMP,
                santa_send_info = ?
            WHERE id = ?
        ''', (send_info, assignment_id))
        
        # Бубенчики за очередность начисляются при закрытии регистрации, а не при отправке
        if updated_existing:
            chat_message = (
                f"Внучок! Данные для получения изменились: {send_info}"
            ).strip()
        else:
            chat_message = (
                f"Дорогой внучок! Я всё отправил! {send_info}\n"
                "Если будут вопросы — пиши!"
            ).strip()
        conn.execute('''
            INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
            VALUES (?, 'santa', ?, NULL)
        ''', (assignment_id, chat_message))

        conn.commit()
        log_activity(
            'assignment_sent',
            details=f'Подарок отправлен по назначению #{assignment_id}',
            metadata={'assignment_id': assignment_id, 'event_id': assignment['event_id']}
        )
        return True, 'Информация об отправке сохранена'
    except Exception as e:
        log_error(f"Error marking assignment sent (id={assignment_id}): {e}")
        conn.rollback()
        return False, 'Не удалось сохранить информацию об отправке'
    finally:
        conn.close()

def mark_assignment_received(assignment_id, user_id, thank_you_message, receipt_file):
    """Отмечает, что подарок получен"""
    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError):
        return False, 'Некорректный идентификатор пользователя'
    
    conn = get_db_connection()
    assignment = conn.execute('SELECT * FROM event_assignments WHERE id = ?', (assignment_id,)).fetchone()
    
    if not assignment:
        conn.close()
        return False, 'Задание не найдено'
    
    if assignment['recipient_user_id'] != user_id_int:
        conn.close()
        return False, 'Вы не можете обновить это задание'
    
    if is_event_finished(assignment['event_id']):
        conn.close()
        return False, 'Мероприятие завершено. Действия с заданием недоступны.'
    
    if not assignment['santa_sent_at']:
        conn.close()
        return False, 'Даритель еще не отметил отправку подарка'
    conn.close()

    thank_you_message = _normalize_multiline_text(thank_you_message, max_length=1000)
    if not thank_you_message:
        return False, 'Напишите спасибо для Деда Мороза.'

    if not receipt_file or not receipt_file.filename:
        return False, 'Приложите фотографию подарка.'

    filename = secure_filename(receipt_file.filename)
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in ALLOWED_LETTER_IMAGE_EXTENSIONS:
        return False, 'Допускается загрузка только изображений (PNG, JPG, JPEG, GIF, WEBP).'

    unique_name = f"{assignment_id}_{int(datetime.now().timestamp())}_{secrets.token_hex(4)}{ext}"
    saved_filepath = os.path.join(ASSIGNMENT_RECEIPT_FOLDER, unique_name)
    try:
        receipt_file.save(saved_filepath)
    except Exception as exc:
        log_error(f"Failed to save assignment receipt image {unique_name}: {exc}")
        return False, 'Не удалось загрузить изображение.'

    receipt_relative_path = f"{ASSIGNMENT_RECEIPT_RELATIVE}/{unique_name}"

    conn = get_db_connection()
    try:
        conn.execute('''
            UPDATE event_assignments
            SET recipient_received_at = CURRENT_TIMESTAMP,
                recipient_thanks_message = ?,
                recipient_receipt_image = ?
            WHERE id = ?
        ''', (thank_you_message, receipt_relative_path, assignment_id))
        conn.commit()
        log_activity(
            'assignment_received',
            details=f'Получение подарка подтверждено по заданию #{assignment_id}',
            metadata={'assignment_id': assignment_id, 'event_id': assignment['event_id']}
        )
        conn.execute('''
            INSERT INTO letter_messages (assignment_id, sender, message, attachment_path)
            VALUES (?, 'grandchild', ?, ?)
        ''', (
            assignment_id,
            f"Дорогой Дед Мороз! Спасибо за подарок! {thank_you_message}",
            receipt_relative_path
        ))
        conn.commit()
        
        # Добавляем автоматический комментарий "спасибо от внучка" в профиль получателя
        assignment_data = conn.execute('SELECT recipient_user_id FROM event_assignments WHERE id = ?', (assignment_id,)).fetchone()
        if assignment_data:
            recipient_id = assignment_data['recipient_user_id']
            thanks_comment = f"Спасибо от внучка: {thank_you_message}" if thank_you_message else "Спасибо от внучка: Подарок получен!"
            add_thanks_comment_from_recipient(recipient_id, assignment_id, thanks_comment)
        
        return True, 'Получение подарка подтверждено'
    except Exception as e:
        log_error(f"Error marking assignment received (id={assignment_id}): {e}")
        conn.rollback()
        try:
            if os.path.exists(saved_filepath):
                os.remove(saved_filepath)
        except OSError:
            pass
        return False, 'Не удалось подтвердить получение подарка'
    finally:
        conn.close()


def _format_full_address(assignment):
    parts = []
    postal = assignment.get('recipient_postal_code')
    country = assignment.get('recipient_country')
    city = assignment.get('recipient_city')
    street = assignment.get('recipient_street')
    house = assignment.get('recipient_house')
    building = assignment.get('recipient_building')
    apartment = assignment.get('recipient_apartment')

    if postal:
        parts.append(str(postal))
    if country:
        parts.append(country)
    if city:
        parts.append(city)

    street_parts = []
    if street:
        street_parts.append(street)
    if house:
        street_parts.append(f"д. {house}")
    if building:
        street_parts.append(f"корп. {building}")
    if apartment:
        street_parts.append(f"кв. {apartment}")

    if street_parts:
        parts.append(', '.join(street_parts))

    if not parts:
        return 'адрес пока не указан'

    return ', '.join(parts)
