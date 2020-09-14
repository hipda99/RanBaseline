from django.core.files.storage import FileSystemStorage
from django.shortcuts import render
import os.path
import sys
# Create your views here.
from environment import CONFIGURATION_PATH
from scr.parser.zte import zte_filler


def index(request):
    context = {}

    if request.method == 'POST' and request.FILES['myfile']:
        uploaded_file = request.FILES['myfile']

        file_path = CONFIGURATION_PATH + uploaded_file.name
        fs = FileSystemStorage(CONFIGURATION_PATH)
        print(CONFIGURATION_PATH)

        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            context['status'] = 'Invalid files.'
            return render(request, 'polls/index.html', context)

        fs.save(uploaded_file.name, uploaded_file)
        context['status'] = 'The file is uploaded.'

        file_name = uploaded_file.name.split('_')

        if file_name[1] == '90018002100':
            zte_filler.run('900/1800/2100', file_name[2])
        else:
            zte_filler.run(int(file_name[1]), file_name[2])


    return render(request, 'polls/index.html', context)

