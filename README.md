# levigoNS

```ASCII

 ___                       ()    __,,,             \|\\   ||"  |====)
  \ \    ___  __      __  ___    | _ \     ____     ||\\  ||   \\
  | |   / ==\  \\    //    ||    \ " ,|,  / __ \    || \\ ||    +++
  | |  | ,--"   \\  //     ||   _/ _ \   | |  | |   ||  \\||       \\
  | |  | |_.    | \/ |     ||   |_ " /    \ \/ /    ||   \||   /|__/ |
 _|_|_  \___\    \__/     _||_   ;;\/      \__/    _||    \|\  \____/

levigoNS ~ The same old leveldb at Go via levigo with NameSpaced Key powers.

```

[![baby-gopher](https://raw2.github.com/drnic/babygopher-site/gh-pages/images/babygopher-badge.png)](http://www.babygopher.org)

### Contributing?

We have set of Go Tasks available here to bring you at pace...

* Install tall Go lib dependencies:
```
./go-tasks.sh deps
```

* Run all Tests:
```
./go-tasks.sh test
```

So, you can have all dependencies set-up in project specific GoEnv and quickly run (updated) tests against your changes.

---

### Basic Logic Used

Set of Key:Val sent to KeyVal Store

>
>  a       => A
>  a:1     => A1
>  a:2     => A2
>  a:3     => A3
>  a:1:2   => A12
>  a:2:1   => A21
>  a:1:1   => A11
>

Representation at KeyVal Store

```ASCII

 ~> a

 key::a      =>  (/)
 val::a      =>  A



 ~> a:1

 key::a      =>  key::a:1
 val::a      =>  A
 key::a:1    =>  (/)
 val::a:1    =>  A1




 ~> a:2

 key::a      =>  key::a:1,key::a:2
 val::a      =>  A
 key::a:1    =>  (/)
 val::a:1    =>  A1
 key::a:2    =>  (/)
 val::a:2    =>  A2




 ~> a:3

 key::a      =>  key::a:1,key::a:2,key::a:3
 val::a      =>  A
 key::a:1    =>  (/)
 val::a:1    =>  A1
 key::a:2    =>  (/)
 val::a:2    =>  A2
 key::a:3    =>  (/)
 val::a:3    =>  A3




 ~> a:1:2

 key::a      =>  key::a:1,key::a:2,key::a:3
 val::a      =>  A
 key::a:1    =>  key::a:1:2
 val::a:1    =>  A1
 key::a:2    =>  (/)
 val::a:2    =>  A2
 key::a:3    =>  (/)
 val::a:3    =>  A3
 key::a:1:2  =>  (/)
 val::a:1:2  =>  A12




 ~> a:2:1

 key::a      =>  key::a:1,key::a:2,key::a:3
 val::a      =>  A
 key::a:1    =>  key::a:1:2
 val::a:1    =>  A1
 key::a:2    =>  key::a:2:1
 val::a:2    =>  A2
 key::a:3    =>  (/)
 val::a:3    =>  A3
 key::a:1:2  =>  (/)
 val::a:1:2  =>  A12
 key::a:2:1  =>  (/)
 val::a:2:1  =>  A21




 ~> a:1:1

 key::a      =>  key::a:1,key::a:2,key::a:3
 val::a      =>  A
 key::a:1    =>  key::a:1:2,key::a:1:1
 val::a:1    =>  A1
 key::a:2    =>  key::a:2:1
 val::a:2    =>  A2
 key::a:3    =>  (/)
 val::a:3    =>  A3
 key::a:1:2  =>  (/)
 val::a:1:2  =>  A12
 key::a:2:1  =>  (/)
 val::a:2:1  =>  A21
 key::a:1:1  =>  (/)
 val::a:1:1  =>  A11

```

