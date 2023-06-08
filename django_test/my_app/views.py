from django.shortcuts import render
from django.http import HttpResponse

# Create your views here.

from .models import MyModel

def index(request):
    output = 'List of MyModel\n<br>' 
    latest = MyModel.objects.order_by('-pub_date')[:5]
    output += ', '.join([q.text for q in latest])
    return HttpResponse(output)
