import random
from datetime import datetime, timedelta

def generate_students(num_students = 10000):
    """
    生成指定数量的学生信息
    """
    students = []
    
    # 常见姓氏和名字
    surnames = ['李', '王', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴', 
                '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗']
    
    male_names = ['伟', '强', '勇', '军', '磊', '超', '鹏', '杰', '建平', '斌', 
                  '浩', '宇', '峰', '晨', '亮', '鑫', '明', '涛', '刚', '健']
    
    female_names = ['芳', '娜', '敏', '静', '秀英', '丽', '艳', '娟', '玲', '霞',
                    '燕', '艳', '梅', '洁', '琳', '颖', '丹', '洁', '君', '婷']
    
    # 年级列表
    grades = ['一年级', '二年级', '三年级', '四年级', '五年级', '六年级',
              '初一', '初二', '初三', '高一', '高二', '高三']
    
    # 地址模板
    cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']
    districts = ['朝阳区', '海淀区', '浦东新区', '福田区', '西湖区', '鼓楼区', '武侯区', '江汉区', '雁塔区', '渝中区']
    streets = ['中山路', '人民路', '解放路', '建设路', '和平路', '新华路', '青年路', '文化路', '科技路', '创业路']
    
    for i in range(1, num_students + 1):
        # 随机性别
        sex = random.choice(['男', '女'])
        
        # 生成姓名（根据性别选择不同的名字库）
        surname = random.choice(surnames)
        if sex == '男':
            name = surname + random.choice(male_names)
        else:
            name = surname + random.choice(female_names)
        
        # 生成年龄（6-18岁之间）
        age = random.randint(6, 18)
        
        # 根据年龄选择年级
        if age <= 6:
            grade = '一年级'
        elif age <= 12:
            grade = random.choice(['一年级', '二年级', '三年级', '四年级', '五年级', '六年级'])
        elif age <= 15:
            grade = random.choice(['初一', '初二', '初三'])
        else:
            grade = random.choice(['高一', '高二', '高三'])
        
        # 生成出生日期（基于年龄推算）
        current_year = datetime.now().year
        birth_year = current_year - age
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # 简单处理，避免2月29日等问题
        birth = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
        
        # 生成手机号
        phone_prefix = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139',
                       '150', '151', '152', '153', '155', '156', '157', '158', '159',
                       '180', '181', '182', '183', '184', '185', '186', '187', '188', '189']
        phone = random.choice(phone_prefix) + ''.join([str(random.randint(0, 9)) for _ in range(8)])
        
        # 生成地址
        city = random.choice(cities)
        district = random.choice(districts)
        street = random.choice(streets)
        number = random.randint(1, 999)
        address = f"{city}市{district}{street}{number}号"
        
        # 生成创建时间（过去1-3年内的随机时间）
        days_ago = random.randint(1, 1095)  # 3年=1095天
        create_time = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
        teacherId = random.randint(1, 1000) 
        
        student = {
            "id": i,
            "name": name,
            "age": age,
            "grade": grade,
            "sex": sex,
            "birth": birth,
            "phone": phone,
            "address": address,
            "createTime": create_time,
            "teacherId": teacherId
        }
        
        students.append(student)
    
    return students


def generate_single_student(id):
    """
    生成指定数量的学生信息
    """
    
    # 常见姓氏和名字
    surnames = ['李', '王', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴', 
                '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗']
    
    male_names = ['伟', '强', '勇', '军', '磊', '超', '鹏', '杰', '建平', '斌', 
                  '浩', '宇', '峰', '晨', '亮', '鑫', '明', '涛', '刚', '健']
    
    female_names = ['芳', '娜', '敏', '静', '秀英', '丽', '艳', '娟', '玲', '霞',
                    '燕', '艳', '梅', '洁', '琳', '颖', '丹', '洁', '君', '婷']
    
    # 年级列表
    grades = ['一年级', '二年级', '三年级', '四年级', '五年级', '六年级',
              '初一', '初二', '初三', '高一', '高二', '高三']
    
    # 地址模板
    cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']
    districts = ['朝阳区', '海淀区', '浦东新区', '福田区', '西湖区', '鼓楼区', '武侯区', '江汉区', '雁塔区', '渝中区']
    streets = ['中山路', '人民路', '解放路', '建设路', '和平路', '新华路', '青年路', '文化路', '科技路', '创业路']
    
    # 随机性别
    sex = random.choice(['男', '女'])
    
    # 生成姓名（根据性别选择不同的名字库）
    surname = random.choice(surnames)
    if sex == '男':
        name = surname + random.choice(male_names)
    else:
        name = surname + random.choice(female_names)
    
    # 生成年龄（6-18岁之间）
    age = random.randint(6, 18)
    
    # 根据年龄选择年级
    if age <= 6:
        grade = '一年级'
    elif age <= 12:
        grade = random.choice(['一年级', '二年级', '三年级', '四年级', '五年级', '六年级'])
    elif age <= 15:
        grade = random.choice(['初一', '初二', '初三'])
    else:
        grade = random.choice(['高一', '高二', '高三'])
    
    # 生成出生日期（基于年龄推算）
    current_year = datetime.now().year
    birth_year = current_year - age
    birth_month = random.randint(1, 12)
    birth_day = random.randint(1, 28)  # 简单处理，避免2月29日等问题
    birth = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
    
    # 生成手机号
    phone_prefix = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139',
                   '150', '151', '152', '153', '155', '156', '157', '158', '159',
                   '180', '181', '182', '183', '184', '185', '186', '187', '188', '189']
    phone = random.choice(phone_prefix) + ''.join([str(random.randint(0, 9)) for _ in range(8)])
    
    # 生成地址
    city = random.choice(cities)
    district = random.choice(districts)
    street = random.choice(streets)
    number = random.randint(1, 999)
    address = f"{city}市{district}{street}{number}号"
    
    # 生成创建时间（过去1-3年内的随机时间）
    days_ago = random.randint(1, 1095)  # 3年=1095天
    create_time = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    teacherId = random.randint(1, 1000) 
    
    student = {
        "id": id,
        "name": name,
        "age": age,
        "grade": grade,
        "sex": sex,
        "birth": birth,
        "phone": phone,
        "address": address,
        "createTime": create_time,
        "teacherId": teacherId
    }
    
    return student

def generate_teachers(num_students = 1000):
    """
    生成指定数量的老师信息
    """
    teachers = []
    
    # 常见姓氏和名字
    surnames = ['李', '王', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴', 
                '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗']
    
    male_names = ['伟', '强', '勇', '军', '磊', '超', '鹏', '杰', '建平', '斌', 
                  '浩', '宇', '峰', '晨', '亮', '鑫', '明', '涛', '刚', '健']
    
    female_names = ['芳', '娜', '敏', '静', '秀英', '丽', '艳', '娟', '玲', '霞',
                    '燕', '艳', '梅', '洁', '琳', '颖', '丹', '洁', '君', '婷']
    
    # 年级列表
    grades = ['一年级', '二年级', '三年级', '四年级', '五年级', '六年级',
              '初一', '初二', '初三', '高一', '高二', '高三']
    
    # 地址模板
    cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '成都', '武汉', '西安', '重庆']
    districts = ['朝阳区', '海淀区', '浦东新区', '福田区', '西湖区', '鼓楼区', '武侯区', '江汉区', '雁塔区', '渝中区']
    streets = ['中山路', '人民路', '解放路', '建设路', '和平路', '新华路', '青年路', '文化路', '科技路', '创业路']
    
    for i in range(1, num_students + 1):
        # 随机性别
        sex = random.choice(['男', '女'])
        
        # 生成姓名（根据性别选择不同的名字库）
        surname = random.choice(surnames)
        if sex == '男':
            name = surname + random.choice(male_names)
        else:
            name = surname + random.choice(female_names)
        
        # 生成年龄（6-18岁之间）
        age = random.randint(23, 60)
        
        # 生成出生日期（基于年龄推算）
        current_year = datetime.now().year
        birth_year = current_year - age
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # 简单处理，避免2月29日等问题
        birth = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
        
        # 生成手机号
        phone_prefix = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139',
                       '150', '151', '152', '153', '155', '156', '157', '158', '159',
                       '180', '181', '182', '183', '184', '185', '186', '187', '188', '189']
        phone = random.choice(phone_prefix) + ''.join([str(random.randint(0, 9)) for _ in range(8)])
        
        # 生成地址
        city = random.choice(cities)
        district = random.choice(districts)
        street = random.choice(streets)
        number = random.randint(1, 999)
        address = f"{city}市{district}{street}{number}号"
        
        # 生成创建时间（过去1-3年内的随机时间）
        days_ago = random.randint(1, 1095)  # 3年=1095天
        create_time = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
        
        teracher = {
            "id": i,
            "name": name,
            "age": age,
            "sex": sex,
            "birth": birth,
            "phone": phone,
            "address": address,
            "createTime": create_time
        }
        
        teachers.append(teracher)
    
    return teachers

