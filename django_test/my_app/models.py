from django.db import models

class MyModel(models.Model):
    text = models.CharField(max_length=200)
    count = models.IntegerField()
    pub_date = models.DateTimeField('date published')
