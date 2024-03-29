# Generated by Django 3.2.9 on 2022-05-23 06:21

import datetime
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('otl_interpreter', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='otljob',
            name='timeout',
            field=models.DurationField(default=datetime.timedelta(0), verbose_name='timeout'),
        ),
        migrations.AlterField(
            model_name='nodejobresult',
            name='status',
            field=models.CharField(choices=[('CALCULATING', 'Result is calculating'), ('CALCULATED', 'Result is calculated'), ('NOT_EXIST', 'Result do not exist')], db_index=True, default='NOT_EXIST', max_length=255, verbose_name='Result status'),
        ),
        migrations.AlterField(
            model_name='nodejobresult',
            name='ttl',
            field=models.DurationField(default=datetime.timedelta(seconds=60)),
        ),
        migrations.AlterField(
            model_name='otljob',
            name='status',
            field=models.CharField(choices=[('NEW', 'New'), ('TRANSLATED', 'Translated'), ('PLANNED', 'Planned'), ('RUNNING', 'Running'), ('FINISHED', 'Finished'), ('CANCELED', 'Canceled'), ('FAILED', 'Failed')], db_index=True, default='NEW', max_length=255),
        ),
        migrations.AlterField(
            model_name='otljob',
            name='ttl',
            field=models.DurationField(default=datetime.timedelta(seconds=60), verbose_name='ttl'),
        ),
    ]
