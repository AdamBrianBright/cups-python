from cups.models import Ability, EnabledAbility, Entity, Group, Perm, Scope


class User(Entity):
    pass


def test_complicated_perms_links(clear_db):
    adam = User.create(name='Adam Bright')
    ivan = User.create(name='Ivan')
    shadow = User.create(name='ShadowVip')
    dude = User.create(name='RandomDude')
    guest = User.create(name='Guest')

    modpack = Scope.create(name='Modpack')
    server = Scope.create(name='Server')
    off_scope = Scope.create(name='Off scope')
    server.subset_of = modpack

    select = Perm.create(name='select')
    create = Perm.create(name='create')
    update = Perm.create(name='update')
    delete = Perm.create(name='delete')
    fly1 = Perm.create(name='fly1')
    fly1.scope = server
    fly2 = Perm.create(name='fly2')
    fly2.scope = modpack

    users = Group.create(name='Users')
    editors = Group.create(name='Editors')
    moderators = Group.create(name='Moderators')
    contributors = Group.create(name='Contributors')
    admins = Group.create(name='Admins')

    users.make_global(force=True)
    contributors.scope = server

    moderators.inherits = editors
    contributors.inherits = moderators
    admins.inherits = moderators

    users.link_perm(select, allow=True)
    editors.link_perm(update, allow=True)
    contributors.link_perm(update, allow=False)
    moderators.link_perm(create, allow=True)
    admins.link_perm(delete, allow=True)

    adam.add_to_group(admins)
    ivan.add_to_group(moderators)
    shadow.add_to_group(editors)
    dude.add_to_group(contributors)

    adam.link_perm(update, allow=False)

    fly = Ability.create(name='Fly')
    fly.scope = modpack
    fly.add_perm_support(fly1)
    fly.add_perm_support(fly2)

    guest.link_perm(fly1, scope=server)
    dude.link_perm(fly2, scope=modpack)
    adam.link_perm(fly1, scope=server)
    ivan.link_perm(fly2, scope=server)
    adam.activate_ability(fly, fly1, scope=server)
    ivan.activate_ability(fly, fly2, scope=modpack)

    # input()

    assert select.id == Perm.get_one(name='select').id

    assert Group.get_global().id == users.id

    assert set(guest.get_allowed_perms()) == {select, fly1}
    assert set(dude.get_allowed_perms()) == {select, create, fly2}
    assert set(shadow.get_allowed_perms()) == {select, update}
    assert set(ivan.get_allowed_perms()) == {select, update, create, fly2}
    assert set(adam.get_allowed_perms()) == {select, create, delete, fly1}

    assert set(guest.get_allowed_perms(scope=server)) == {select, fly1}
    assert set(dude.get_allowed_perms(scope=server)) == {select, create, fly2}
    assert set(shadow.get_allowed_perms(scope=server)) == {select, update}
    assert set(ivan.get_allowed_perms(scope=server)) == {select, update, create, fly2}
    assert set(adam.get_allowed_perms(scope=server)) == {select, create, delete, fly1}

    assert set(guest.get_allowed_perms(scope=modpack)) == {select}
    assert set(dude.get_allowed_perms(scope=modpack)) == {select, fly2}
    assert set(shadow.get_allowed_perms(scope=modpack)) == {select, update}
    assert set(ivan.get_allowed_perms(scope=modpack)) == {select, update, create}
    assert set(adam.get_allowed_perms(scope=modpack)) == {select, create, delete}

    assert set(guest.get_allowed_perms(scope=off_scope)) == {select}
    assert set(dude.get_allowed_perms(scope=off_scope)) == {select}
    assert set(shadow.get_allowed_perms(scope=off_scope)) == {select, update}
    assert set(ivan.get_allowed_perms(scope=off_scope)) == {select, update, create}
    assert set(adam.get_allowed_perms(scope=off_scope)) == {select, create, delete}

    assert adam.is_able(select)
    assert adam.is_able(select, server)
    assert adam.is_able(select, modpack)

    assert adam.is_able(fly1)
    assert adam.is_able(fly1, server)
    assert not adam.is_able(fly1, modpack)

    assert not adam.is_able(fly2)
    assert not adam.is_able(fly2, server)
    assert not adam.is_able(fly2, modpack)

    assert set(adam.get_activated_abilities(server)) == {EnabledAbility(fly, fly1.id, server.id)}
