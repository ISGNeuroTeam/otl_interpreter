# Generated by Django 3.2.9 on 2022-10-10 05:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('otl_interpreter', '0003_auto_20220525_1504'),
    ]

    operations = [
        migrations.AlterField(
            model_name='nodecommand',
            name='resource_necessity',
            field=models.JSONField(blank=True, null=True),
        ),
    ]
