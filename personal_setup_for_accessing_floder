def personal_setup():
    input_dir = os.path.abspath('path_to_folder')
    cmd = "C:/Windows/System32/net.exe use Z: path_to_folder /persistent:yes /user:%s %s %(username, password)"
    success = os.system(cmd)

    if success !=1:
        print("You do not have an access to the folder")
        
    processed_dir = os.path.join(input_dir, 'PROCESSED')
    return input_dir, processed_dir
