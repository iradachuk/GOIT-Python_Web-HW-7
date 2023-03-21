from sqlalchemy import func, desc, and_, select

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_1():
    '''
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    '''
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2(discipline_id: int):
    '''
    Знайти студента із найвищим середнім балом з певного предмета.
    :return:
    '''
    result = session.query(Discipline.name, Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).join(Discipline).filter(Discipline.id == discipline_id)\
                    .group_by(Student.id, Discipline.name).order_by(desc('avg_grade')).limit(1).all()
    return result


def select_3(discipline_id: int):
    '''
    Знайти середній бал у групах з певного предмета.
    :return:
    '''
    result = session.query(Discipline.name, Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).join(Discipline).join(Group)\
                    .filter(Discipline.id == discipline_id).group_by(Group.id, Discipline.id)\
                    .order_by(desc('avg_grade')).all()
    return result


def select_4(group_id: int):
    '''
    Знайти середній бал на потоці (по всій таблиці оцінок).
    :return:
    '''
    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Grade).join(Student).join(Group).filter(Group.id == group_id)\
                    .group_by(Group.name).all()
    return result


def select_5(teacher_id: int):
    '''
    Знайти які курси читає певний викладач.
    :return:
    '''
    result = session.query(Discipline.name).select_from(Teacher).join(Discipline)\
                    .filter(Teacher.id == teacher_id).all()
    return result


def select_6(group_id: int):
    '''
    Знайти список студентів у певній групі.
    :return:
    '''
    result = session.query(Student.fullname).select_from(Student).join(Group)\
                    .filter(Group.id == group_id).order_by(Student.fullname).all()
    return result


def select_7(group_id: int, discipline_id: int):
    '''
    Знайти оцінки студентів у окремій групі з певного предмета.
    :return:
    '''
    result = session.query(Student.fullname, Grade.grade).select_from(Grade)\
                    .join(Student).join(Discipline).join(Group)\
                    .filter(Group.id == group_id and Discipline.id == discipline_id)\
                    .order_by(Discipline.name).all()
    return result


def select_8(teacher_id: int):
    '''
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    :return:
    '''
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade'), Discipline.name, Teacher.fullname)\
                    .select_from(Grade).join(Discipline).join(Teacher)\
                    .filter(Teacher.id == teacher_id)\
                    .group_by(Discipline.name, Teacher.fullname).all()
    return result


def select_9(student_id: int):
    '''
    Знайти список курсів, які відвідує певний студент.
    :return:
    '''
    result = session.query(Discipline.name).select_from(Grade).join(Discipline).join(Student)\
                    .filter(Student.id == student_id).group_by(Discipline.name).all()
    return result


def select_10(student_id: int, teacher_id: int):
    '''
    Список курсів, які певному студенту читає певний викладач.
    :return:
    '''
    result = session.query(Discipline.name).select_from(Grade)\
                    .join(Discipline).join(Student).join(Teacher)\
                    .filter(Student.id == student_id and Teacher.id == teacher_id).group_by(Discipline.name).all()
    return result


def select_11(student_id: int, teacher_id: int):
    '''
    Середній бал, який певний викладач ставить певному студентові.
    :return:
    '''
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Student).join(Discipline).join(Teacher)\
        .filter(Student.id == student_id and Teacher.id == teacher_id).group_by(Student.fullname).all()
    return result


def select_12(discipline_id: int, group_id: int):
    '''
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    :return:
    '''
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    result = session.query(Discipline.name,
                           Student.fullname,
                           Group.name,
                           Grade.date_of,
                           Grade.grade) \
        .select_from(Grade).join(Student).join(Discipline).join(Group)\
        .filter(and_(Discipline.id == discipline_id), Group.id == group_id, Grade.date_of == subquery)\
        .order_by(desc(Grade.date_of)).all()
    return result



if __name__ == '__main__':
    print(select_1())
    print(select_2(2))
    print(select_3(1))
    print(select_4(2))
    print(select_5(5))
    print(select_6(1))
    print(select_7(3, 4))
    print(select_8(4))
    print(select_9(5))
    print(select_10(4, 2))
    print(select_11(4, 2))
    print(select_12(1, 1))
