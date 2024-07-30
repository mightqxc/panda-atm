cwlVersion: v1.2
class: Workflow

requirements:
  MultipleInputFeatureRequirement: {}

inputs:
  signal: string
  background: string

outputs:
  outDS:
    type: string
    outputSource: combine/outDS


steps:
  make_signal:
    run: prun
    in:
      opt_inDS: signal
      opt_containerImage:
        default: docker://busybox
      opt_exec:
        default: "echo %IN > abc.dat; echo 123 > def.zip"
      opt_args:
        default: "--outputs abc.dat,def.zip --nFilesPerJob 5"
    out: [outDS]

  make_background_1:
    run: prun
    in:
      opt_inDS: background
      opt_exec:
        default: "echo %IN > opq.root; echo %IN > xyz.pool"
      opt_args:
        default: "--outputs opq.root,xyz.pool --nGBPerJob 10"
    out: [outDS]

  premix:
    run: prun
    in:
      opt_inDS: make_signal/outDS
      opt_inDsType:
        default: def.zip
      opt_secondaryDSs: [make_background_1/outDS]
      opt_secondaryDsTypes:
        default: [xyz.pool]
      opt_exec:
        default: "echo %IN %IN2 > klm.root"
      opt_args:
        default: "--outputs klm.root --secondaryDSs IN2:2:%{SECDS1}"
    out: [outDS]

  generate_some:
    run: prun
    in:
      opt_exec:
        default: "echo %RNDM:10 > gen.root"
      opt_args:
        default: "--outputs gen.root --nJobs 10"
    out: [outDS]

  make_background_2:
    run: prun
    in:
      opt_inDS: background
      opt_containerImage:
        default: docker://alpine
      opt_secondaryDSs: [generate_some/outDS]
      opt_secondaryDsTypes:
        default: [gen.root]
      opt_exec:
        default: "echo %IN > ooo.root; echo %IN2 > jjj.txt"
      opt_args:
        default: "--outputs ooo.root,jjj.txt --secondaryDSs IN2:2:%{SECDS1}"
    out: [outDS]

  combine:
    run: prun
    in:
      opt_inDS: make_signal/outDS
      opt_inDsType:
        default: abc.dat
      opt_secondaryDSs: [premix/outDS, make_background_2/outDS]
      opt_secondaryDsTypes:
        default: [klm.root, ooo.root]
      opt_exec:
        default: "echo %IN %IN2 %IN3 > aaa.root"
      opt_args:
        default: "--outputs aaa.root --secondaryDSs IN2:2:%{SECDS1},IN3:5:%{SECDS2}"
    out: [outDS]

~                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
~                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
~                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
~                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
~                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
~                                  
