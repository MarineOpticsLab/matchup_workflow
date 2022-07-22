# Guide for using Charlie in the Mitchell Lab

## 0. Connecting to Charlie

[The wiki](https://charlie.bigelow.org/) has lots of great info and walkthroughs for getting up and running, including this page about [connecting to Charlie](https://charlie.bigelow.org/connect-and-transfer/connecting).

## 1. Basic Set-up & Usage Protocols

### 1.1 File systems

The current protocol is to not use your home directory in Charlie for storing files. Therefore, 
 `\\storage.bigelow.org\labs\mitchell` is mounted in Charlie: `/mnt/storage/labs/mitchell/`. For brevity, 
 in this file, `s~/` (i.e. "storage home") is shorthand for `/mnt/storage/labs/mitchell/` (vs 
 `~` which is your home directory on Charlie).

### 1.2 Running code

Everything should be run through the scheduler: PBS. Nothing should be run on cfe (Charlie's front end), it should be run 
on one of the other nodes (e.g. c1, c2, c3). Detailed instructions on how to submit jobs through PBS can be found on the 
[Charlie wiki](https://charlie.bigelow.org/documentation). This wiki also contains lots of other information about using Charlie.

#### Interactive sessions

If you want to use [JupyterLab on Charlie](https://charlie.bigelow.org/software/data-science/jupyter-notebooks), you need to set up an interactive session through the PBS scheduler. An interactive session means you have access to the requested resources and can work on Charlie in real-time. This is also useful for script development and debugging. See later for examples.

### 1.3 Anaconda environments

If you need to install a Python package that isn't currently on Charlie, it's good practice to set up your own conda environment,
and install the packages you want there - AND do this on a project-by-project basis. There are instructions on how to do this on
 [the wiki](https://charlie.bigelow.org/conda-environments). 
 
### 1.4 `Screen` sessions

 The `screen` command opens a new window, where you can set something running, and then you can push 
that window to the background (detach from your screen). This is great for long running processes because once you've 
pushed your screen session to the background, you can close your terminal window (i.e. disconnect from Charlie), and the process 
will keep running. Then, when you log back into Charlie, you can open your screen session again and see how it's doing.

Useful `screen` commands:
* `screen` will open a screen session
* In an active screen session, pressing Ctrl+A, then releasing and pressing d will deattach the screen
* `screen -r` will reatatch a screen
* In an active screen session, `exit` will close it

### 1.5 Creating and editing files at the terminal

There are some text editors that you can use within a terminal session/at the command line. One of these is
`vi`, which is one of the default Unix text editors. 

To open a file in `vi` run:

`vi filename`

This will open a file called `filename` within your terminal window. If the file didn't exist before, a new, empty file of that name will be opened.

If the file is empty, there will be lots of `~` down the side of the window and the first line should say `# filename`. 
`vi` is a modal editor - you can either be in `insert` mode, where 
anything you type becomes text in the file, or in `command` mode, where different keys are different commands
e.g. selecting parts of text, copying, saving, quiting, etc. There are loads of resources online to explore
the full capabilities of `vi`, but a brief overview is detailed below.

1. Once you've opened the file, enter `insert` mode by pressing `i` on your keyboard. At the bottom of 
the window you should now see `-- INSERT --`. Now you can use the arrow keys to navigate around the file and 
any letter/number keys will be added as text. 
2. Move to a blank line below the first line (hit the return key a few times to create some more blank lines if
you need to).
3. Type whatever you'd like into your file
4. Exit `insert` mode by pressing `Esc`
5. Save and quit the file by pressing `:wq` (the `w` saves, the `q` quits)
6. If you make a mistake (or try editing the file before entering `insert` mode), you can exit without saving
by pressing `:q!`
 
### 1.6 Examples of daily setup and usage

Here's a couple of workflows used by Cath:

#### 1.6.1 JupyterLab Sessions

More often than not, I'm running JupyterLab on Charlie. You can go through the steps on the wiki for [setting up a JupyterLab session](https://charlie.bigelow.org/software/data-science/jupyter-notebooks). Or, if you don't want to type all the commands every time, you can set up a couple of bash scripts and call those.

My workflow is:

1. Connect to Charlie
2. From cfe (just after logging in) run `./interactivepbs.sh` (script below)
3. Once the job has started, run `./jupterlabsteup.sh` (script below)
4. Then, once the script has completed, copy and paste one of the urls into a browser to connect to my JupyterLab session

**`interactivepbs.sh`**
```
#!/opt/pbs/bin/qsub

#PBS -I
#PBS -l walltime=08:00:00,ncpus=4,mem=32gb
#PBS -N cath-i
#PBS -l model=c3
```

Note in the above script I'm specifying space on c3. If there is a lot of usage by others and you have trouble connecting / starting an interactive job, you can specify either the `devel` or `low` queues because c3-18 is reserved for only `devel` or `low` priority jobs. `devel` jobs will jump to the front of the line, but have only a limited amount of resources available (check the wiki), and `low` priority jobs will be queued behind other `low` priority jobs (if there are others), but don't have a limit on the resources requested. To specify the queue within a PBS submission script, add the following to your script:

```
#PBS -q devel
```

e.g. the above could be added as the last line in **`interactivepbs.sh`**.

**`jupterlabsetup.sh`**
```
module load anaconda3
source activate ~/cathsenv/
unset XDG_RUNTIME_DIR
cd /mnt/storage/labs/mitchell/
hostname=$( cat /etc/hostname )
key=${hostname##*-}
ipaddress=10.10.3.$key
echo $ipaddress
jupyter lab --no-browser --port=8890 --ip=$ipaddress
```

I have both of these scripts saved in a `scripts` directory in my home directory on Charlie. If you want to make these scripts, you can follow the instructions in Section 1.5 for creating files in the terminal and put the above text in the files. If you want to make the files in a `scripts` directory, you would `cd` into that directory and then call e.g. `vi interactivepbs.sh` to the script.

#### 1.6.2 Script Development

*An example for working on satellite processing code*

I use the following command to set up my session:

`qsub -I -l ncpus=8,mem=32,walltime=08:00:00 -N cath-sat`

NB: walltime is the length of time the interactive session will be active (8 hrs in the above case), and ncpus are the number of
cpus you want. I then load anaconda and activate my ocssw environment (this is a conda environment that I have setup that contains all the relevant packages needed for running the satellite processing code, details in section 2.3):

```
module load anaconda3
source activate ~/ocssw_env
```

Then I edit scripts and run them in real-time without having to put them in the scheduler queue. It's useful to do this for smaller, debugging, or testing projects, rather than submit to the scheduler and wait and see what happens. 

#### 1.6.3 Dask workflow

At one point I was trying out Dask. I didn't really get it to do what I wanted properly, so haven't been using it. But I made the following notes at the time I was testing it so I'm keeping them here in case someone wants to try out Dask again in the future.

If you want to run Python code that uses `dask`, this needs to be run from cfe. This is because when using `dask`, 
you request jobs via `dask` Python commands, within your Python script. It will interface with PBS so that the jobs are actually 
run on one of the other nodes, but the basic Python script that sets-up the `dask` cluster will be run on cfe. This script should
be small and not use a lot of resources. **An important note when running a script in this way (i.e. from cfe):** you should run
them in a `screen` session.

When I'm adding dask into scripts and testing them, I do that from cfe i.e. I don't start with a call to `qsub` to set up an 
interactive session. I just load anaconda and my enviroment and am ready to test scripts:

```
module load anaconda3
source activate ~/cathsenv
```


## 2. SeaDAS processing code installation

The SeaDAS `ocssw` processing bundle should be installed into your home directory. 

Full instructions are [provided by NASA OBPG](https://seadas.gsfc.nasa.gov/downloads/#manual-installation).

To download the installer script and manifest script on Charlie, use the `wget` command with the full url for each file. You can get the url by hovering over the installer script and manifest script links on the NASA OBPG page linked above. At the time of writing the following would be the commands to run:

`wget https://oceandata.sci.gsfc.nasa.gov/manifest/install_ocssw`

`wget https://oceandata.sci.gsfc.nasa.gov/manifest/manifest.py`

I put these files in the following directory `~/scripts/`

Run the installer script via one of the following options:

1) Change directory to directory where it is saved then run:

`./install_ocssw --install_dir ~/ocssw --tag V2022.0 --seawifs --aqua --terra --viirsn --viirsj1 --src --seadas`

OR 

2) In your home directory (`~`) run:

`./path/to/file/install_ocssw --install_dir ~/ocssw --tag V2022.0 --seawifs --aqua --terra --viirsn --viirsj1 --src --seadas`

NB: in both of the above, we're installing the V2022.0 version of the code. This is often updated, so check what version number you want to install. To see the available versions (tags) run

`./install_ocssw --list_tags` 

Then take the last in the list that is of the form V20XX.X

### 2.1 Adding OCSSW to path

Add the ocssw code to your `.bashrc` file (which is in your home directory) by 
adding the following lines to the file (more detailed instructions on editing files at the terminal are in Section 1.5):

```
export OCSSWROOT=~/ocssw
source $OCSSWROOT/OCSSW_bash.env
```

The above assumes you installed the ocssw code into your home directory. If you didn't change the path in OCSSWROOT accordingly. 

NB: Source/further info here: https://seadas.gsfc.nasa.gov/downloads/#CommandLineConfig 

**In recent installs, Cath has found OCSSW_bash.env in a subfolder `~/ocssw/ocssw_src/`. I've moved it into the OCSSWROOT folder so the `source` command above works.**

Also add the below lines to your `.bash_profile` file: 

```
if [ -f ~/.bashrc ]; then
        . ~/.bashrc
fi
```


### 2.2 EarthData Login Credentials

You have to login every time you want to download data, so you needed to put your user credentials into a 
file which you provide whenever you do a `wget` call. To do this, type the following at the terminal:

```
echo "machine urs.earthdata.nasa.gov login USERNAME password PASSWD" > ~/.netrc ; > ~/.urs_cookies
chmod  0600 ~/.netrc
```

where `USERNAME` and `PSSWD` are your login details to EarthData. The first line creates the file with the 
relevant information and the second line is changing the permissions on the file.

NB: These instructions are from https://oceancolor.gsfc.nasa.gov/data/download_methods. There's an example in 
the `wget` section of that webpage about how to call your `.netrc` file when you make a `wget` call.


### 2.3 Anaconda environment

Finally, some of the ocssw code relies on some python packages that weren't already installed on Charlie, 
so set up an environment and add the requests package. At the terminal, do the following:

```
module load anaconda3
conda create --prefix ~/ocssw_env
source activate ~/ocssw_env
conda install requests
```
Here, we are loading the anaconda3 module into our session so we can use conda commands. Then we are 
creating a new environment in our home directory called `ocssw_env`, activating it, then installing the
requests package.